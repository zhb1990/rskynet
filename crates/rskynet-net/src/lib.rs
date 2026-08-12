//! rskynet 的网络层，对照 `skynet-src/socket_server.c` 与 `service_socket.c`。
//!
//! **本 crate 目前是空的**：crate 划分与内核扩展点先落地，网络本体另开一轮。
//!
//! 形态已经定下来了，都靠 [`rskynet_core::ext`] 那三件扩展点支撑：
//!
//! - 一条 socket 线程（[`rskynet_core::Plugin`] 的 `run`）驱动 mio 的 `Poll`，
//!   对照 C 版的 `thread_socket`。`shutdown` 钩子敲 mio 的 `Waker` 把它叫醒收工。
//! - socket 事件用 [`rskynet_core::NodeRef::send`] 以 `MsgType::SOCKET`
//!   （即 `PTYPE_SOCKET`，协议号 6）投给持有该连接的服务，与定时器回包同一条路径。
//! - 服务侧的 `listen` / `connect` / `send` / `close` 走
//!   [`rskynet_core::Ctx::call_external`]：命令连同一个
//!   [`rskynet_core::ReplyToken`] 交给 socket 线程，办完回话，于是调用方写成
//!   一句 `await`。这对应 C 版那条命令管道加 `PTYPE_RESPONSE` 回包。
//! - `SocketServer`（命令队列 + mio `Waker`）在 `Plugin::init` 里进扩展槽，
//!   服务侧的扩展 trait 靠 [`rskynet_core::NodeRef::extension`] 取回来。
//! - 监听地址等配置走 `[net]` 段，见 `rskynet_core::Config::section`。
