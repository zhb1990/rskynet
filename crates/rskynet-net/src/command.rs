//! 业务服务跟网络层说话的那套命令，对照 `socket_server.c` 里走命令管道的
//! `'L'` / `'O'` / `'D'` / `'K'` 那一组。
//!
//! C 版把命令编成「1 字节类型 + 定长结构体」压进一根 pipe，socket 线程在 `poll`
//! 里连管道一起等。这里管道就是网络层服务自己的邮箱，命令是一条
//! [`MsgType::USER`] 消息，办完 `ctx.reply`——于是调用方那侧是一句 `await`：
//!
//! ```ignore
//! let id = net::listen(&ctx, "0.0.0.0:8888").await?;
//! net::start(&ctx, id).await?;
//! ```
//!
//! 下面那些函数就是这个意思，[`Command`] 与 [`Answer`] 是它们内部走的协议。直接
//! 用枚举也行（想给网络层换个名字、或者同时跑两个网络层服务时只能这么办），但
//! 类型化的函数编译期能查错，是推荐的写法。

use std::net::SocketAddr;

use rskynet_core::{Ctx, Error, FromPayload, MsgType, Payload, Result, boxed_payload};

use crate::NAME;
use crate::event::{SocketId, SocketInfo};

/// 一条给网络层的命令。
///
/// 以 [`MsgType::USER`] 投给网络层服务（默认名字 `.net`）。绝大多数时候不必直接
/// 碰它，用本模块那些函数就行。
#[derive(Debug)]
pub enum Command {
    /// 绑好一个监听口，对照 `'L'`。回来的 socket 还没开始 accept。
    Listen { addr: String },
    /// 连一个地址，对照 `'O'`。连上（或失败）才回话。
    Connect { addr: String },
    /// 与 `Connect` 相同，但限制 TCP 建连等待时间。
    ConnectWithTimeout { addr: String, timeout_ms: u64 },
    /// 把 socket 放进 poll 并把属主改成发命令的人，对照 `'S'` / `'R'`。
    Start(SocketId),
    /// 掐掉读事件，对照 `socket_server_pause`。
    Pause(SocketId),
    /// 发数据，对照 `'D'`（高优先）与 `'P'`（低优先）。
    Send {
        id: SocketId,
        data: Vec<u8>,
        high: bool,
    },
    /// 与 `Send` 相同，但写队列触及高水位时等它回落后才应答。
    SendWait {
        id: SocketId,
        data: Vec<u8>,
        high: bool,
    },
    /// 写完欠的数据再关，对照 `'K'`。
    Close(SocketId),
    /// 立刻关，欠的数据不要了，对照 `socket_server_shutdown`。
    Shutdown(SocketId),
    /// 开关 `TCP_NODELAY`，对照 `'T'`。
    NoDelay { id: SocketId, on: bool },
    /// 开一个 UDP 端口，对照 `'U'`。`bind` 不写就让系统挑个端口。
    Udp { bind: Option<String> },
    /// 为指定对端建立 UDP 端点，默认绑定与对端相同的地址族。
    UdpForPeer { addr: String },
    /// 记下 UDP 的默认对端，对照 `'C'`。
    UdpConnect { id: SocketId, addr: String },
    /// 发一个 UDP 包，对照 `'A'`。`to` 不写就发给默认对端。
    UdpSend {
        id: SocketId,
        to: Option<SocketAddr>,
        data: Vec<u8>,
    },
    /// 与 `UdpSend` 相同，但写队列越过高水位时等待回落。
    UdpSendWait {
        id: SocketId,
        to: Option<SocketAddr>,
        data: Vec<u8>,
    },
    /// 问一个 socket 的现状，对照 `'Q'`。
    Info(SocketId),
    /// 枚举全部活跃 socket 及其统计，对照 `socket.netstat()`。
    Netstat,
    #[doc(hidden)]
    ConnectTimeoutElapsed(SocketId),
}

boxed_payload!(Command);

/// 网络层给命令的回话。
///
/// 失败走 [`Answer::Failed`] 而不是 `MsgType::ERROR`：后者只能表达「不成」，
/// 原因就丢了，而「地址被占」和「域名不存在」对调用方是两件事。
#[derive(Debug)]
pub enum Answer {
    /// 新建的 socket 编号。
    Id(SocketId),
    Info(SocketInfo),
    Infos(Vec<SocketInfo>),
    /// 办好了，没什么要回的。
    Done,
    Failed(String),
}

boxed_payload!(Answer);

/// 绑一个监听口，但**还不开始 accept**。
///
/// 与 C 版 `socket_server_listen` 一样分两步：这里只把端口占下来，得再调一次
/// [`start`] 才开始收连接。分开是为了让「谁当属主」由调用方定——`start` 的调用方
/// 才是收 [`Accept`][crate::SocketEvent::Accept] 事件的人。
///
/// `addr` 可以是 `"0.0.0.0:8888"` 这样的字面地址，也可以是要解析的域名；后者会
/// 转给一条专门的解析线程，不会卡住网络层。
///
/// 端口写 0 就让系统挑一个，挑中哪个用 [`info`] 问。
///
/// # backlog
///
/// C 版能指定 backlog，这里不能：mio 没开这个旋钮，用的是它自己的默认值。
pub async fn listen(ctx: &Ctx, addr: impl Into<String>) -> Result<SocketId> {
    ask_id(ctx, Command::Listen { addr: addr.into() }).await
}

/// 连一个地址，**连上了才返回**。
///
/// C 版的 `socket_server_connect` 立刻返回 id，连上与否另发一条 `SOCKET_OPEN`；
/// 这里把那条回包并进了 `await`，语义与 Lua 那侧的 `socket.open` 一致。
///
/// 返回之后连接已经在 poll 里了，数据会以 [`Data`][crate::SocketEvent::Data]
/// 事件投给调用方，不必再 [`start`]。
/// 域名解析出多个 IPv4 / IPv6 地址时会按顺序尝试，直到某个地址连通或全部失败。
pub async fn connect(ctx: &Ctx, addr: impl Into<String>) -> Result<SocketId> {
    ask_id(ctx, Command::Connect { addr: addr.into() }).await
}

/// 连一个地址，并在 `timeout_ms` 后仍未连通时失败并回收底层 socket。
pub async fn connect_timeout(
    ctx: &Ctx,
    addr: impl Into<String>,
    timeout_ms: u64,
) -> Result<SocketId> {
    ask_id(
        ctx,
        Command::ConnectWithTimeout {
            addr: addr.into(),
            timeout_ms,
        },
    )
    .await
}

/// 开始收数据，并把属主改成自己。
///
/// 三种场合用它，对照 C 版的 `'S'`：
///
/// - [`listen`] 拿到的监听口：从此开始 accept。
/// - [`Accept`][crate::SocketEvent::Accept] 事件里那个新连接：从此开始收数据。
///   通常由**接手这个连接的那个服务**来调，于是第一个字节就投对了人。
/// - [`pause`] 过的连接：恢复读。
pub async fn start(ctx: &Ctx, id: SocketId) -> Result<()> {
    ask_done(ctx, Command::Start(id)).await
}

/// 先别读了，对照 `socket_server_pause`。
///
/// 属主处理不过来时用它挡一挡：网络层不再从内核缓冲往外搬数据，压力顺着 TCP
/// 窗口传回对端。想接着读就再调一次 [`start`]。
pub async fn pause(ctx: &Ctx, id: SocketId) -> Result<()> {
    ask_done(ctx, Command::Pause(id)).await
}

/// 发数据，不等回话，对照 C 版的 `'D'`。
///
/// 能直接写进内核缓冲的部分立刻就写了，剩下的攒在写缓冲里等可写事件。攒得太多
/// 会收到 [`Warning`][crate::SocketEvent::Warning]。
pub fn send(ctx: &Ctx, id: SocketId, data: Vec<u8>) -> Result<()> {
    tell(
        ctx,
        Command::Send {
            id,
            data,
            high: true,
        },
    )
}

/// 同 [`send`]，但排在所有高优先数据之后，对照 C 版的 `'P'`。
///
/// 大块的、晚一点无妨的数据走它，别把握手与踢人通知堵在后面。
pub fn send_low(ctx: &Ctx, id: SocketId, data: Vec<u8>) -> Result<()> {
    tell(
        ctx,
        Command::Send {
            id,
            data,
            high: false,
        },
    )
}

/// 发送数据，并在底层写队列拥塞时等待它回落到低水位。
pub async fn send_wait(ctx: &Ctx, id: SocketId, data: Vec<u8>) -> Result<()> {
    ask_done(
        ctx,
        Command::SendWait {
            id,
            data,
            high: true,
        },
    )
    .await
}

/// 低优先级版本的 [`send_wait`]。
pub async fn send_low_wait(ctx: &Ctx, id: SocketId, data: Vec<u8>) -> Result<()> {
    ask_done(
        ctx,
        Command::SendWait {
            id,
            data,
            high: false,
        },
    )
    .await
}

/// 把欠的数据写完再关，对照 C 版的 `'K'`。
///
/// 写缓冲空了才真的关掉，所以这个 `await` 可能要等一会儿。关掉之后属主还会收到
/// 一条 [`Close`][crate::SocketEvent::Close]——「socket 没了」只有这一个信号，
/// 自己关的也不例外。
pub async fn close(ctx: &Ctx, id: SocketId) -> Result<()> {
    ask_done(ctx, Command::Close(id)).await
}

/// 立刻关，欠的数据不要了，对照 `socket_server_shutdown`。
pub fn shutdown(ctx: &Ctx, id: SocketId) -> Result<()> {
    tell(ctx, Command::Shutdown(id))
}

/// 开关 `TCP_NODELAY`，对照 C 版的 `'T'`。
pub fn set_nodelay(ctx: &Ctx, id: SocketId, on: bool) -> Result<()> {
    tell(ctx, Command::NoDelay { id, on })
}

/// 开一个 UDP 端口，对照 `socket_server_udp`。
///
/// 与 TCP 不同，它一建好就在 poll 里了，收到的包直接以
/// [`Udp`][crate::SocketEvent::Udp] 事件投给调用方，不必再 [`start`]。
/// `bind` 不写就绑 `0.0.0.0:0`，用 [`info`] 问系统挑了哪个端口。
pub async fn udp(ctx: &Ctx, bind: Option<&str>) -> Result<SocketId> {
    ask_id(
        ctx,
        Command::Udp {
            bind: bind.map(str::to_string),
        },
    )
    .await
}

/// 建立一个已记住默认对端的 UDP 端点。
///
/// 域名会先在解析线程中解析，再根据选中的对端绑定 IPv4 或 IPv6
/// wildcard 地址；没有可用路由或无法建立端点时会继续尝试后续候选，避免首个
/// A / AAAA 结果不可达，或默认 IPv4 socket 无法向 IPv6 对端发包。
pub async fn udp_for_peer(ctx: &Ctx, addr: impl Into<String>) -> Result<SocketId> {
    ask_id(ctx, Command::UdpForPeer { addr: addr.into() }).await
}

/// 记下默认对端，之后 [`udp_send`] 不写地址就发给它，对照 `socket_server_udp_connect`。
pub async fn udp_connect(ctx: &Ctx, id: SocketId, addr: impl Into<String>) -> Result<()> {
    ask_done(
        ctx,
        Command::UdpConnect {
            id,
            addr: addr.into(),
        },
    )
    .await
}

/// 发一个 UDP 包，对照 C 版的 `'A'`。
///
/// `to` 不写就发给 [`udp_connect`] 记下的那个对端；两个都没有就报错。
pub fn udp_send(ctx: &Ctx, id: SocketId, to: Option<SocketAddr>, data: Vec<u8>) -> Result<()> {
    tell(ctx, Command::UdpSend { id, to, data })
}

/// 发送一个 UDP 包，并在底层写队列拥塞时等待它回落。
///
/// 等待只影响调用方，包边界始终保留。
pub async fn udp_send_wait(
    ctx: &Ctx,
    id: SocketId,
    to: Option<SocketAddr>,
    data: Vec<u8>,
) -> Result<()> {
    ask_done(ctx, Command::UdpSendWait { id, to, data }).await
}

/// 问一个 socket 的现状，对照 C 版的 `'Q'`。
pub async fn info(ctx: &Ctx, id: SocketId) -> Result<SocketInfo> {
    match ask(ctx, Command::Info(id)).await? {
        Answer::Info(info) => Ok(info),
        other => Err(unexpected(other)),
    }
}

/// 枚举网络层中全部活跃 socket 及其统计，按 [`SocketId`] 升序返回。
///
/// 返回 listener、TCP、UDP 以及尚未 `start`、正在连接、半关闭等过渡态。
/// 已经关闭并释放槽位的 socket 不会出现在结果中。
pub async fn netstat(ctx: &Ctx) -> Result<Vec<SocketInfo>> {
    match ask(ctx, Command::Netstat).await? {
        Answer::Infos(infos) => Ok(infos),
        other => Err(unexpected(other)),
    }
}

/// 下一条命令并等回话。
async fn ask(ctx: &Ctx, cmd: Command) -> Result<Answer> {
    let reply = ctx.call(NAME, MsgType::USER, Payload::of(cmd)).await?;
    match Answer::from_payload(reply)? {
        // 把原因带回调用方，而不是只说一句「不成」
        Answer::Failed(reason) => Err(Error::Service(reason)),
        answer => Ok(answer),
    }
}

/// 下一条命令，不等回话。
fn tell(ctx: &Ctx, cmd: Command) -> Result<()> {
    ctx.send(NAME, MsgType::USER, Payload::of(cmd))
}

async fn ask_id(ctx: &Ctx, cmd: Command) -> Result<SocketId> {
    match ask(ctx, cmd).await? {
        Answer::Id(id) => Ok(id),
        other => Err(unexpected(other)),
    }
}

async fn ask_done(ctx: &Ctx, cmd: Command) -> Result<()> {
    match ask(ctx, cmd).await? {
        Answer::Done => Ok(()),
        other => Err(unexpected(other)),
    }
}

fn unexpected(answer: Answer) -> Error {
    Error::Service(format!("网络层回了个对不上的应答：{answer:?}"))
}
