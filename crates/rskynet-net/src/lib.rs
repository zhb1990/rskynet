//! rskynet 的网络层，对照 `skynet-src/socket_server.c` 与 `service_socket.c`。
//!
//! 它是一个[独占线程的服务][rskynet_core::Exclusive]，用
//! [`Registry::with_exclusive`][rskynet_core::Registry::with_exclusive] 注册
//! （现成的写法见 [`RegistryExt`]）。C 版的 `thread_socket` 在这里不需要内核给
//! 什么特殊待遇——它要的东西，服务本来都有：
//!
//! - **那条线程**：独占服务自带。`idle` 里 `poll.poll(events, None)` 阻塞等 IO，
//!   醒来把事件派发掉；`interrupt` 敲 mio 的 `Waker`，好让邮箱里的命令也叫得动它。
//! - **命令**：[`listen`] / [`connect`] / [`send`] / [`close`] 都是发给本服务的
//!   消息，办完 `ctx.reply`，于是调用方那侧写成一句 `await`。这对应 C 版那根命令
//!   管道加 `PTYPE_RESPONSE` 回包。
//! - **统计**：[`info`] 查询单个 socket，[`netstat`] 枚举全部活跃 socket，包含
//!   Skynet 的累计收发、accept、最后收发时间、写缓冲与读写状态，并补充属主
//!   service 的 handle、类型和本地名字。
//! - **事件**：socket 事件以 [`MsgType::SOCKET`][rskynet_core::MsgType::SOCKET]
//!   （协议号 5）投给持有该连接的服务，与定时器回包同一条路径。
//! - **配置**：`[net]` 段在 `init` 里读，见 [`NetConfig`]。
//!
//! 域名解析要另起一条线程（[`ToSocketAddrs`][std::net::ToSocketAddrs] 是阻塞的），
//! 那条线程正是靠 [`ext`][rskynet_core::ext] 那两件东西回话的：
//! [`ReplyToken`][rskynet_core::ReplyToken] 给一次 `call` 回包，于是网络层这侧写成
//! 一句 `ctx.call_external(..).await`，挂起期间照常收发。
//!
//! # 用起来是这样
//!
//! 使用 `rskynet` 门面时，配置段存在就会在业务引导前自动拉起：
//!
//! ```toml
//! [net]
//! max_socket = 65536
//!
//! [bootstrap]
//! services = [{ name = "echo" }]
//! ```
//!
//! 直接使用 `rskynet-core::Builder` 时，则用 `startup_service("net", "")` 明确加入
//! timer 与 bootstrap 之间的启动项。
//!
//! 然后一个回声服务器长这样：
//!
//! ```ignore
//! use rskynet::net::{self, SocketEvent};
//!
//! #[rskynet::service]
//! impl Echo {
//!     async fn init(&self, ctx: Ctx) -> Result<()> {
//!         let id = net::listen(&ctx, "0.0.0.0:8888").await?;
//!         net::start(&ctx, id).await?;      // 从此开始 accept
//!         Ok(())
//!     }
//!
//!     #[msg(MsgType::SOCKET)]
//!     async fn on_socket(&self, ctx: Ctx, event: SocketEvent) {
//!         match event {
//!             // 新连接默认不收数据，start 一下才开始；这个空档正好用来转交属主
//!             SocketEvent::Accept { id, .. } => { let _ = net::start(&ctx, id).await; }
//!             SocketEvent::Data { id, data } => { let _ = net::send(&ctx, id, data); }
//!             _ => {}
//!         }
//!     }
//! }
//! ```
//!
//! # 与 C 版有意不同的几处
//!
//! - **地址就是 [`SocketAddr`][std::net::SocketAddr]**，不做 C 版那套「1 字节
//!   协议族 + IP + 端口」的字节编码——那是为了让 Lua 把地址当字符串拿着。
//! - **写缓冲只收 `Vec<u8>`**，没有 C 版 `send_object` 那层「用户对象 + 取指针
//!   回调」的间接：同进程传对象在 rskynet 里走
//!   [`Payload::Boxed`][rskynet_core::Payload]，压根不必经过网络层。
//! - **`listen` 不收 backlog**：mio 没开这个旋钮。
//! - **[`connect`] 连上了才返回**，而不是先返回 id、再等一条 `SOCKET_OPEN`。
//!   语义与 Lua 那侧的 `socket.open` 一致。
//! - **告警阈值在写缓冲排空时复位**，于是下一波堆积还会报一次；C 版的 `warn_size`
//!   只增不减，报过一轮之后就不再吭声了。
//!
//! # 源码对照
//!
//! | 本 crate | skynet | 内容 |
//! | --- | --- | --- |
//! | `service.rs` | `socket_server_poll` / `skynet_socket.c` | 那条线程的主循环、命令处理、事件派发 |
//! | `socket.rs` | `struct socket` / `HASH_ID` | 槽位表与状态机 |
//! | `buffer.rs` | `wb_list` / `p.size` | 写缓冲双队列与读缓冲自适应 |
//! | `command.rs` | 命令管道那一组 | [`Command`] 与类型化的调用函数 |
//! | `event.rs` | `SOCKET_DATA` 那一组 | [`SocketEvent`] |
//! | `config.rs` | `MAX_SOCKET_P` 等常量 | [`NetConfig`] |
//! | `resolve.rs` | 无对应 | 域名解析线程 |

mod buffer;
mod command;
mod config;
mod event;
mod resolve;
mod service;
mod socket;

use rskynet_core::Registry;

pub use command::{
    Answer, Command, close, connect, connect_timeout, info, listen, netstat, pause, send, send_low,
    send_low_wait, send_wait, set_nodelay, shutdown, start, udp, udp_connect, udp_for_peer,
    udp_send, udp_send_wait,
};
pub use config::NetConfig;
pub use event::{SocketEvent, SocketId, SocketInfo};
pub use service::NetService;

/// 网络层服务的约定类型名，同时也是它注册的名字（`.net`）。
///
/// 与 `logger` / `timer` / `bootstrap` 不同，内核**不认**这个名字：网络层不是系统
/// 服务，得在 `[bootstrap]` 的清单里点名才会被拉起。
pub const NAME: &str = "net";

/// 把网络层挂进注册表。
pub trait RegistryExt {
    /// 用约定的名字 [`NAME`] 注册 [`NetService`]。
    ///
    /// 只是注册类型，不会拉起它——什么时候起、起几个，由 `[bootstrap]` 的清单说。
    #[must_use]
    fn with_net(self) -> Self;
}

impl RegistryExt for Registry {
    fn with_net(self) -> Self {
        self.with_exclusive(NAME, NetService::new)
    }
}
