//! rskynet 的网络层，对照 `skynet-src/socket_server.c` 与 `service_socket.c`。
//!
//! **本 crate 目前是空的**：crate 划分与内核那侧的接缝先落地，网络本体另开一轮。
//!
//! 形态已经定下来了——它就是一个[独占线程的服务][rskynet_core::Exclusive]，
//! 用 [`rskynet_core::Registry::with_exclusive`] 注册。C 版的 `thread_socket`
//! 在这里不需要内核给什么特殊待遇，它需要的东西服务本来都有：
//!
//! - **那条线程**：独占服务自带。[`rskynet_core::Exclusive::idle`] 里
//!   `poll.poll(events, None)` 阻塞等 IO，醒来把事件派发掉；
//!   [`rskynet_core::Exclusive::interrupt`] 敲 mio 的 `Waker`，好让邮箱里的
//!   命令也能把它叫回来。收工不必另设钩子：服务被摘除时内核自会叫醒它。
//! - **命令**：`listen` / `connect` / `send` / `close` 都是发给本服务的消息，
//!   办完 `ctx.reply`，调用方于是写成一句 `await`。这对应 C 版那条命令管道加
//!   `PTYPE_RESPONSE` 回包。
//! - **事件**：socket 事件用 `ctx.send` 以 `MsgType::SOCKET`（即 `PTYPE_SOCKET`，
//!   协议号 6）投给持有该连接的服务，与定时器回包同一条路径。
//! - **配置**：`[net]` 段在 `init` 里用 `ctx.node().section("net")` 读。
//!
//! 只有真要再起子线程时（比如把阻塞的域名解析挪出去）才用得上
//! [`rskynet_core::ext`]：[`rskynet_core::NodeRef`] 往邮箱投消息，
//! [`rskynet_core::ReplyToken`] 给一次 `call` 回话。
