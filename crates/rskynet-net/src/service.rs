//! 网络层服务本体，对照 `socket_server.c` 的 `socket_server_poll` 与
//! `skynet-src/skynet_socket.c`。
//!
//! 它是一个[独占线程的服务][rskynet_core::Exclusive]，所以 C 版 `thread_socket`
//! 那条专用线程在这里不需要内核给任何特殊待遇：
//!
//! | C 版 | 这里 |
//! | --- | --- |
//! | `thread_socket` 那条线程 | 独占服务自带的那条线程，`idle` 就是 `poll.poll(.., None)` |
//! | 命令管道（pipe）与它在 epoll 里的那个 fd | 服务自己的邮箱；`interrupt` 敲 mio 的 `Waker` 把 `poll` 叫回来 |
//! | `skynet_socket_*` 直接碰内核结构 | 命令是消息，办完 `ctx.reply` |
//! | `SOCKET_DATA` 等等塞进 `skynet_context_push` | `ctx.send` 一条 `MsgType::SOCKET` |
//!
//! # 谁在哪条线程上
//!
//! `init` / `dispatch` / `idle` 全都跑在本服务那条线程上，所以槽位表、写缓冲、
//! `Poll` 一律放 [`SvcCell`]，不需要锁。唯一的例外是 [`Exclusive::interrupt`]：
//! 它由**任意线程**调用（谁给本服务投消息谁调），所以它只许碰 `Arc<Waker>` 那一个
//! 字段，别的一概不能摸。

use std::collections::VecDeque;
use std::io::{ErrorKind, Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use mio::event::Event;
use mio::net::{TcpListener, TcpStream, UdpSocket};
use mio::{Events, Poll, Registry, Token, Waker};

use rskynet_core::{Ctx, Error, Idler, Message, MsgType, Payload, Result, SvcCell, log};

use crate::NAME;
use crate::buffer::Chunk;
use crate::command::{Answer, Command};
use crate::config::NetConfig;
use crate::event::{SocketEvent, SocketId};
use crate::resolve::{Resolved, Resolver};
use crate::socket::{Kind, Pending, Sockets, State, WAKE};

/// 一个 UDP 包最多这么大，对照 C 版 `MAX_UDP_PACKAGE`。
const MAX_UDP_PACKAGE: usize = 65535;

/// `bind` 没写时 UDP 绑哪儿。
const ANY_UDP: &str = "0.0.0.0:0";

/// 关一个 socket 时怎么跟属主交代。
enum Farewell {
    /// 什么都不说。用于「压根没连上过」的连接——发起方那句 `await` 已经拿到错误了，
    /// 再投一条事件只是噪音。
    Silent,
    /// 投一条 [`SocketEvent::Close`]。
    Closed,
    /// 投一条 [`SocketEvent::Error`]。
    Failed(String),
}

/// 事件该按哪种东西来处理。
#[derive(Clone, Copy)]
enum Role {
    Listener,
    Stream,
    Udp,
}

/// 一次写的结果。「欠的都写完了」不在这里——那是 `wb.next()` 返回 `None`，直接
/// 跳出循环。
enum Step {
    /// 写出去了一些，接着写。
    Wrote,
    /// 内核缓冲满了，等下一次可写事件。
    Blocked,
    Failed(String),
}

/// 地址得先解析才办得了的命令。
enum Deferred {
    Listen,
    Connect,
    Udp,
    UdpConnect { id: SocketId },
}

/// 网络层服务，用 [`Registry::with_exclusive`][rskynet_core::Registry::with_exclusive]
/// 注册，现成的写法见 [`crate::RegistryExt`]。
pub struct NetService {
    /// [`Exclusive::interrupt`] 唯一碰得的东西。mio 的 `Waker` 接得住早到的唤醒
    /// ——唤醒可能发生在 `poll` 真正睡下去之前，那一下不能丢。
    waker: Arc<Waker>,
    /// `Poll` 的注册端，`dispatch` 拿它把新 socket 挂进去。`Registry` 是 `Sync` 的，
    /// 所以不必与 `Poll` 抢同一个借用。
    registry: Registry,
    poll: SvcCell<Poll>,
    events: SvcCell<Events>,
    sockets: SvcCell<Sockets>,
    /// 懒起的域名解析线程：只用 IP 地址的节点压根不会有这条线程。
    resolver: SvcCell<Option<Resolver>>,
    config: SvcCell<NetConfig>,
}

impl NetService {
    /// 建一个网络层服务。
    ///
    /// # Panics
    ///
    /// `mio::Poll` 建不起来时 panic。注册表收的工厂是 `Fn() -> S`，没有报错的口子；
    /// 而这一步失败只可能是 fd 耗尽或内核不支持 epoll/kqueue，那种节点本来也起不来。
    pub fn new() -> Self {
        Self::try_new().expect("mio 的 Poll 建不起来，网络层无从下手")
    }

    fn try_new() -> std::io::Result<Self> {
        let poll = Poll::new()?;
        let waker = Arc::new(Waker::new(poll.registry(), WAKE)?);
        let registry = poll.registry().try_clone()?;
        Ok(Self {
            waker,
            registry,
            poll: SvcCell::new(poll),
            // 容量是配置项，`init` 读完配置再换成正主
            events: SvcCell::new(Events::with_capacity(1)),
            sockets: SvcCell::new(Sockets::default()),
            resolver: SvcCell::new(None),
            config: SvcCell::new(NetConfig::default()),
        })
    }
}

impl Default for NetService {
    fn default() -> Self {
        Self::new()
    }
}

#[rskynet_macros::exclusive(
    crate = ::rskynet_core,
    name = "net",
    factory = NetService::new
)]
impl NetService {
    async fn init(&self, ctx: Ctx, args: String) -> Result<()> {
        let config: NetConfig = ctx.node().section(NAME)?.unwrap_or_default();
        config.validate()?;
        self.sockets
            .replace(Sockets::new(config.max_socket, config.min_read_buffer));
        self.events.replace(Events::with_capacity(config.events));
        self.config.replace(config);

        // 名字默认是约定的 `.net`；参数里写了别的就用那个，于是一个节点里能跑
        // 两套网络层（比如内外网各一套）
        let name = match args.trim() {
            "" => NAME,
            given => given,
        };
        if !ctx.register_name(name) {
            return Err(Error::Service(format!("名字 `.{name}` 已经被占了")));
        }
        Ok(())
    }

    async fn dispatch(&self, ctx: Ctx, mut msg: Message) {
        if msg.mtype != MsgType::USER {
            log!(
                ctx,
                "网络层只认 MsgType::USER 的命令，收到的是 {:?}",
                msg.mtype
            );
            let _ = ctx.reply_error(&msg);
            return;
        }
        let waiting = Pending {
            source: msg.source,
            session: msg.session,
        };
        match msg.take_payload().downcast::<Command>() {
            Ok(command) => self.perform(&ctx, *command, waiting),
            Err(_) => {
                log!(ctx, "网络层收到认不出的命令负载，来自 :{:08x}", msg.source);
                let _ = ctx.reply_error(&msg);
            }
        }
    }

    /// 阻塞在 `poll` 上等 IO，醒来把事件派发成消息。
    ///
    /// 超时给的是 `None`（无限等）：邮箱里的命令靠 [`Exclusive::interrupt`] 敲
    /// `Waker` 把这一觉叫醒，被 kill 与节点收工也都会走那条路（内核每次唤醒都是
    /// 先 `unpark` 再 `interrupt`），所以不需要定时睁眼。
    fn idle(&self, ctx: &Ctx, idler: &Idler) {
        let mut poll = self.poll.borrow_mut();
        let mut events = self.events.borrow_mut();
        match poll.poll(&mut events, None) {
            Ok(()) => {}
            // 信号打断了这一觉，回去重扫一遍邮箱就好
            Err(err) if err.kind() == ErrorKind::Interrupted => return,
            Err(err) => {
                log!(ctx, "网络层 poll 出错：{err}");
                // 别在出错时空转：这种错通常不会自愈，喘一口再试
                idler.park_timeout(Duration::from_millis(100));
                return;
            }
        }
        // 派发要动槽位表、还可能改注册，别把 Poll 的借用扣着
        drop(poll);
        for event in events.iter() {
            self.on_event(ctx, event);
        }
    }

    /// 把 `poll` 从阻塞里敲出来。**会被任意线程调用**，所以只碰 `waker`。
    fn interrupt(&self) {
        // 早到的唤醒不会丢：mio 的 Waker 自己记着，下一次 poll 立刻返回
        let _ = self.waker.wake();
    }
}

// ------------------------------------------------------------ 事件派发

impl NetService {
    fn on_event(&self, ctx: &Ctx, event: &Event) {
        if event.token() == WAKE {
            // 内核说邮箱里有活，回去看就是了
            return;
        }
        let Some((id, role)) = self.identify(event.token()) else {
            return;
        };

        // 先办可写：连接刚建立时可读可写可能一起到，得先把「连上了」定下来
        if event.is_writable() {
            self.on_writable(ctx, id);
        }
        if event.is_readable() {
            match role {
                Role::Listener => self.on_acceptable(ctx, id),
                Role::Stream => self.on_readable(ctx, id),
                Role::Udp => self.on_udp(ctx, id),
            }
        }
        // 只报错不报可读可写的情形（有的平台会这样）在这里兜住
        if event.is_error() && self.sockets.borrow().get(id).is_some() {
            let reason = self
                .take_error(id)
                .unwrap_or_else(|| "连接出错".to_string());
            self.close(ctx, id, Farewell::Failed(reason));
        }
    }

    fn identify(&self, token: Token) -> Option<(SocketId, Role)> {
        let sockets = self.sockets.borrow();
        let socket = sockets.by_token(token)?;
        let role = match &socket.kind {
            Kind::Listener(_) => Role::Listener,
            Kind::Stream(_) => Role::Stream,
            Kind::Udp(_) => Role::Udp,
        };
        Some((socket.id, role))
    }

    /// 监听口可读 = 有新连接。收下来之后它还不在 poll 里，等属主 `start`。
    fn on_acceptable(&self, ctx: &Ctx, listen: SocketId) {
        loop {
            let accepted = {
                let sockets = self.sockets.borrow();
                let Some(socket) = sockets.get(listen) else {
                    return;
                };
                // 暂停就是「先不收」，连接排在内核的 backlog 队列里
                if socket.paused {
                    return;
                }
                let Kind::Listener(listener) = &socket.kind else {
                    return;
                };
                (socket.owner, listener.accept())
            };
            let (owner, accepted) = accepted;
            let (stream, peer) = match accepted {
                Ok(pair) => pair,
                // 边沿触发，得一直 accept 到没有为止
                Err(err) if err.kind() == ErrorKind::WouldBlock => return,
                Err(err) if err.kind() == ErrorKind::Interrupted => continue,
                Err(err) => {
                    log!(ctx, "{listen} accept 出错：{err}");
                    return;
                }
            };

            // 属主先跟着监听口，等它（或它指定的那个服务）调 start 时再改
            let fresh = self
                .sockets
                .borrow_mut()
                .insert(owner, Kind::Stream(stream), State::PreAccept)
                .map(|socket| socket.id);
            let Some(id) = fresh else {
                // 槽位满了，这条连接只能就地丢掉（fd 随 stream 一起析构）
                log!(ctx, "槽位已满，{peer} 的连接只好拒掉");
                continue;
            };
            let _ = ctx.send(
                owner,
                MsgType::SOCKET,
                Payload::of(SocketEvent::Accept { id, listen, peer }),
            );
        }
    }

    /// 连接可读。一直读到 WouldBlock：mio 是边沿触发的，读不干净就等不到下一次事件。
    fn on_readable(&self, ctx: &Ctx, id: SocketId) {
        loop {
            enum Got {
                Eof,
                Data { owner: u32, data: Vec<u8> },
                Stop,
                Retry,
                Failed(String),
            }

            let got = {
                let mut sockets = self.sockets.borrow_mut();
                let Some(socket) = sockets.get_mut(id) else {
                    return;
                };
                // 暂停就是「不去搬」：数据压在内核缓冲里，压力顺着 TCP 窗口回到对端
                if socket.paused {
                    return;
                }
                let size = socket.read.size();
                let mut buf = vec![0u8; size];
                let read = match &mut socket.kind {
                    Kind::Stream(stream) => stream.read(&mut buf),
                    _ => return,
                };
                match read {
                    Ok(0) => Got::Eof,
                    Ok(n) => {
                        buf.truncate(n);
                        socket.read.observe(n);
                        socket.stat.read_bytes += n as u64;
                        Got::Data {
                            owner: socket.owner,
                            data: buf,
                        }
                    }
                    Err(err) if err.kind() == ErrorKind::WouldBlock => Got::Stop,
                    Err(err) if err.kind() == ErrorKind::Interrupted => Got::Retry,
                    Err(err) => Got::Failed(format!("读出错：{err}")),
                }
            };

            match got {
                Got::Eof => return self.peer_closed(ctx, id),
                Got::Retry => continue,
                Got::Stop => return,
                Got::Failed(reason) => return self.close(ctx, id, Farewell::Failed(reason)),
                Got::Data { owner, data } => {
                    let _ = ctx.send(
                        owner,
                        MsgType::SOCKET,
                        Payload::of(SocketEvent::Data { id, data }),
                    );
                    // 边沿触发必须读到 WouldBlock。短读只说明这一刻拿到的比缓冲小，
                    // 不保证 readiness 已被排空（Windows IOCP 尤其如此）。
                }
            }
        }
    }

    /// UDP 端口可读。一个包一条事件，长度上限与 C 版一致。
    fn on_udp(&self, ctx: &Ctx, id: SocketId) {
        let mut buf = vec![0u8; MAX_UDP_PACKAGE];
        loop {
            let got = {
                let sockets = self.sockets.borrow();
                let Some(socket) = sockets.get(id) else {
                    return;
                };
                let Kind::Udp(udp) = &socket.kind else {
                    return;
                };
                (socket.owner, udp.recv_from(&mut buf))
            };
            let (owner, got) = got;
            match got {
                Ok((len, from)) => {
                    if let Some(socket) = self.sockets.borrow_mut().get_mut(id) {
                        socket.stat.read_bytes += len as u64;
                    }
                    let _ = ctx.send(
                        owner,
                        MsgType::SOCKET,
                        Payload::of(SocketEvent::Udp {
                            id,
                            from,
                            data: buf[..len].to_vec(),
                        }),
                    );
                }
                Err(err) if err.kind() == ErrorKind::WouldBlock => return,
                Err(err) if err.kind() == ErrorKind::Interrupted => continue,
                Err(err) => {
                    // UDP 上的错误是单个包的事，端口本身还能用，报一声就完了
                    log!(ctx, "{id} 收 UDP 出错：{err}");
                    return;
                }
            }
        }
    }

    fn on_writable(&self, ctx: &Ctx, id: SocketId) {
        let connecting = self
            .sockets
            .borrow()
            .get(id)
            .is_some_and(|socket| socket.state == State::Connecting);
        if connecting {
            self.finish_connect(ctx, id);
        } else {
            self.flush(ctx, id);
        }
    }

    /// 可写事件宣布 `connect` 的结果，对照 C 版的 `report_connect`。
    fn finish_connect(&self, ctx: &Ctx, id: SocketId) {
        let outcome = {
            let mut sockets = self.sockets.borrow_mut();
            let Some(socket) = sockets.get_mut(id) else {
                return;
            };
            // 连接出错时错误挂在 socket 上；peer_addr 拿不到也算没连上
            let failed = match &mut socket.kind {
                Kind::Stream(stream) => stream
                    .take_error()
                    .ok()
                    .flatten()
                    .or_else(|| stream.peer_addr().err()),
                _ => None,
            };
            let waiting = socket.pending.take();
            let fallbacks = std::mem::take(&mut socket.connect_fallbacks);
            match failed {
                Some(err) => Err((waiting, fallbacks, format!("连接失败：{err}"))),
                None => {
                    socket.state = State::Connected;
                    match socket.apply(&self.registry) {
                        Ok(()) => Ok(waiting),
                        Err(err) => Err((
                            waiting,
                            VecDeque::new(),
                            format!("连接成功，但更新 poll 注册失败：{err}"),
                        )),
                    }
                }
            }
        };
        match outcome {
            Ok(waiting) => {
                if let Some(waiting) = waiting {
                    reply(ctx, waiting, Answer::Id(id));
                }
            }
            Err((waiting, fallbacks, reason)) => {
                // 压根没连上过，属主不必收「连接断了」——发起方那句 await 已经知道了
                self.close(ctx, id, Farewell::Silent);
                if let Some(waiting) = waiting {
                    if fallbacks.is_empty() {
                        reply(ctx, waiting, Answer::Failed(reason));
                    } else if let Some(answer) = self.do_connect_candidates(ctx, fallbacks, waiting)
                    {
                        reply(ctx, waiting, answer);
                    }
                }
            }
        }
    }

    /// 对端关了写。欠的数据还是要写完，写完再收摊，对照 C 版的
    /// `SOCKET_TYPE_HALFCLOSE_READ`。
    fn peer_closed(&self, ctx: &Ctx, id: SocketId) {
        let still_writing = {
            let mut sockets = self.sockets.borrow_mut();
            let Some(socket) = sockets.get_mut(id) else {
                return;
            };
            if socket.wb.is_empty() {
                false
            } else {
                socket.state = State::HalfCloseRead;
                let _ = socket.apply(&self.registry);
                true
            }
        };
        if !still_writing {
            self.close(ctx, id, Farewell::Closed);
        }
    }

    /// 把写缓冲往外倒，对照 C 版的 `send_buffer`。
    fn flush(&self, ctx: &Ctx, id: SocketId) {
        loop {
            let step = {
                let mut sockets = self.sockets.borrow_mut();
                let Some(socket) = sockets.get_mut(id) else {
                    return;
                };
                let Some(chunk) = socket.wb.next() else {
                    break;
                };
                let to = chunk.to();
                let rest = chunk.rest();
                // `rest` 借的是 wb 这个字段，下面借的是 kind，两者不冲突
                let written = match &mut socket.kind {
                    Kind::Stream(stream) => stream.write(rest),
                    Kind::Udp(udp) => match to {
                        Some(addr) => udp.send_to(rest, addr),
                        None => Err(ErrorKind::InvalidInput.into()),
                    },
                    Kind::Listener(_) => return,
                };
                match written {
                    Ok(0) => Step::Failed("对端不再接收数据".to_string()),
                    Ok(n) => {
                        socket.wb.consume(n);
                        socket.stat.write_bytes += n as u64;
                        Step::Wrote
                    }
                    Err(err) if err.kind() == ErrorKind::WouldBlock => Step::Blocked,
                    Err(err) if err.kind() == ErrorKind::Interrupted => Step::Wrote,
                    Err(err) => Step::Failed(format!("写出错：{err}")),
                }
            };
            match step {
                Step::Wrote => continue,
                Step::Blocked => {
                    // 等下一次可写事件，兴趣里得有 WRITABLE
                    if let Some(socket) = self.sockets.borrow_mut().get_mut(id) {
                        let _ = socket.apply(&self.registry);
                    }
                    return;
                }
                Step::Failed(reason) => return self.close(ctx, id, Farewell::Failed(reason)),
            }
        }

        // 欠的都写完了：该关的关掉，还活着的把 WRITABLE 摘掉
        let finish = {
            let mut sockets = self.sockets.borrow_mut();
            let Some(socket) = sockets.get_mut(id) else {
                return;
            };
            match socket.state {
                State::HalfCloseWrite | State::HalfCloseRead => Some(socket.pending.take()),
                _ => {
                    let _ = socket.apply(&self.registry);
                    None
                }
            }
        };
        if let Some(waiting) = finish {
            self.close(ctx, id, Farewell::Closed);
            if let Some(waiting) = waiting {
                reply(ctx, waiting, Answer::Done);
            }
        }
    }

    /// 关掉一个 socket 并跟属主交代一声，对照 C 版的 `force_close`。
    fn close(&self, ctx: &Ctx, id: SocketId, farewell: Farewell) {
        let removed = self.sockets.borrow_mut().remove(id);
        let Some(mut socket) = removed else {
            return;
        };
        socket.detach(&self.registry);
        let owner = socket.owner;
        // 半路被掀了的时候，等回话的人也得知道
        if let Some(waiting) = socket.pending.take() {
            match &farewell {
                Farewell::Failed(reason) => reply(ctx, waiting, Answer::Failed(reason.clone())),
                _ => reply(ctx, waiting, Answer::Done),
            }
        }
        // fd 在这里真正关掉
        drop(socket);

        let event = match farewell {
            Farewell::Silent => return,
            Farewell::Closed => SocketEvent::Close { id },
            Farewell::Failed(reason) => SocketEvent::Error { id, reason },
        };
        let _ = ctx.send(owner, MsgType::SOCKET, Payload::of(event));
    }

    fn take_error(&self, id: SocketId) -> Option<String> {
        let mut sockets = self.sockets.borrow_mut();
        let socket = sockets.get_mut(id)?;
        let err = match &mut socket.kind {
            Kind::Listener(listener) => listener.take_error(),
            Kind::Stream(stream) => stream.take_error(),
            Kind::Udp(udp) => udp.take_error(),
        };
        err.ok().flatten().map(|err| err.to_string())
    }
}

// ------------------------------------------------------------ 命令

impl NetService {
    fn perform(self: &Arc<Self>, ctx: &Ctx, command: Command, waiting: Pending) {
        let answer = match command {
            Command::Listen { addr } => match parse(&addr) {
                Some(addr) => Some(self.do_listen(addr, waiting.source)),
                None => return self.defer(ctx, addr, waiting, Deferred::Listen),
            },
            Command::Connect { addr } => match parse(&addr) {
                Some(addr) => self.do_connect(ctx, addr, waiting),
                None => return self.defer(ctx, addr, waiting, Deferred::Connect),
            },
            Command::Udp { bind } => {
                let bind = bind.unwrap_or_else(|| ANY_UDP.to_string());
                match parse(&bind) {
                    Some(addr) => Some(self.do_udp(addr, waiting.source)),
                    None => return self.defer(ctx, bind, waiting, Deferred::Udp),
                }
            }
            Command::UdpConnect { id, addr } => match parse(&addr) {
                Some(addr) => Some(self.do_udp_connect(id, addr)),
                None => return self.defer(ctx, addr, waiting, Deferred::UdpConnect { id }),
            },
            Command::Start(id) => Some(self.do_start(ctx, id, waiting.source)),
            Command::Pause(id) => Some(self.do_pause(id)),
            Command::Send { id, data, high } => Some(self.do_send(ctx, id, Chunk::tcp(data), high)),
            Command::UdpSend { id, to, data } => Some(self.do_udp_send(ctx, id, to, data)),
            Command::Close(id) => self.do_close(ctx, id, waiting),
            Command::Shutdown(id) => {
                self.close(ctx, id, Farewell::Closed);
                Some(Answer::Done)
            }
            Command::NoDelay { id, on } => Some(self.do_nodelay(id, on)),
            Command::Info(id) => Some(self.do_info(id)),
        };
        if let Some(answer) = answer {
            reply(ctx, waiting, answer);
        }
    }

    fn do_listen(&self, addr: SocketAddr, owner: u32) -> Answer {
        let listener = match TcpListener::bind(addr) {
            Ok(listener) => listener,
            Err(err) => return Answer::Failed(format!("绑定 {addr} 失败：{err}")),
        };
        // 只占端口，不进 poll：等属主调 start 才开始 accept，与 C 版的 PLISTEN 一致
        match self
            .sockets
            .borrow_mut()
            .insert(owner, Kind::Listener(listener), State::PreListen)
        {
            Some(socket) => Answer::Id(socket.id),
            None => Answer::Failed("槽位已满，开不了新的监听口".to_string()),
        }
    }

    /// 返回 `None` 表示「等可写事件宣布结果再回话」。
    fn do_connect(&self, ctx: &Ctx, addr: SocketAddr, waiting: Pending) -> Option<Answer> {
        self.do_connect_candidates(ctx, VecDeque::from([addr]), waiting)
    }

    /// 逐个尝试解析出的地址。非阻塞 connect 已经发出后，余下地址记在槽位里；
    /// 可写事件确认当前地址失败时再接着试。
    fn do_connect_candidates(
        &self,
        ctx: &Ctx,
        mut addrs: VecDeque<SocketAddr>,
        waiting: Pending,
    ) -> Option<Answer> {
        let mut last_error = None;
        while let Some(addr) = addrs.pop_front() {
            let stream = match TcpStream::connect(addr) {
                Ok(stream) => stream,
                Err(err) => {
                    last_error = Some(format!("连 {addr} 失败：{err}"));
                    continue;
                }
            };
            let started = {
                let mut sockets = self.sockets.borrow_mut();
                let Some(socket) =
                    sockets.insert(waiting.source, Kind::Stream(stream), State::Connecting)
                else {
                    return Some(Answer::Failed("槽位已满，连不了新的连接".to_string()));
                };
                socket.pending = Some(waiting);
                socket.connect_fallbacks = addrs;
                let id = socket.id;
                socket
                    .apply(&self.registry)
                    .map(|()| id)
                    .map_err(|err| (id, err))
            };
            return match started {
                Ok(_) => None,
                Err((id, err)) => {
                    self.close(ctx, id, Farewell::Silent);
                    Some(Answer::Failed(format!("注册进 poll 失败：{err}")))
                }
            };
        }
        Some(Answer::Failed(last_error.unwrap_or_else(|| {
            "解析结果是空的，无法建立连接".to_string()
        })))
    }

    fn do_udp(&self, addr: SocketAddr, owner: u32) -> Answer {
        let udp = match UdpSocket::bind(addr) {
            Ok(udp) => udp,
            Err(err) => return Answer::Failed(format!("绑定 UDP {addr} 失败：{err}")),
        };
        let mut sockets = self.sockets.borrow_mut();
        // UDP 没有握手，建好就能收，所以不必像 TCP 那样等一次 start
        let Some(socket) = sockets.insert(owner, Kind::Udp(udp), State::Connected) else {
            return Answer::Failed("槽位已满，开不了新的 UDP 端口".to_string());
        };
        match socket.apply(&self.registry) {
            Ok(()) => Answer::Id(socket.id),
            Err(err) => {
                let id = socket.id;
                sockets.remove(id);
                Answer::Failed(format!("注册进 poll 失败：{err}"))
            }
        }
    }

    fn do_udp_connect(&self, id: SocketId, addr: SocketAddr) -> Answer {
        let mut sockets = self.sockets.borrow_mut();
        let Some(socket) = sockets.get_mut(id) else {
            return missing(id);
        };
        if !matches!(socket.kind, Kind::Udp(_)) {
            return Answer::Failed(format!("{id} 不是 UDP 端口"));
        }
        socket.udp_peer = Some(addr);
        Answer::Done
    }

    fn do_start(&self, ctx: &Ctx, id: SocketId, owner: u32) -> Answer {
        let outcome = {
            let mut sockets = self.sockets.borrow_mut();
            let Some(socket) = sockets.get_mut(id) else {
                return missing(id);
            };
            // start 同时是「转移属主」：从此事件投给发命令的这个服务
            socket.owner = owner;
            let resumed = std::mem::replace(&mut socket.paused, false);
            socket.state = match socket.state {
                State::PreListen => State::Listen,
                State::PreAccept => State::Connected,
                keep => keep,
            };
            let role = match socket.kind {
                Kind::Listener(_) => Role::Listener,
                Kind::Stream(_) => Role::Stream,
                Kind::Udp(_) => Role::Udp,
            };
            socket.apply(&self.registry).map(|()| (resumed, role))
        };
        match outcome {
            Ok((resumed, role)) => {
                // 暂停期间攒下的东西不会再来一次事件（边沿触发，那一下早响过了），
                // 得主动补一次
                if resumed {
                    match role {
                        Role::Listener => self.on_acceptable(ctx, id),
                        Role::Stream => self.on_readable(ctx, id),
                        Role::Udp => self.on_udp(ctx, id),
                    }
                }
                Answer::Done
            }
            Err(err) => {
                let reason = format!("{id} 注册进 poll 失败：{err}");
                self.close(ctx, id, Farewell::Failed(reason.clone()));
                Answer::Failed(reason)
            }
        }
    }

    fn do_pause(&self, id: SocketId) -> Answer {
        let mut sockets = self.sockets.borrow_mut();
        let Some(socket) = sockets.get_mut(id) else {
            return missing(id);
        };
        socket.paused = true;
        match socket.apply(&self.registry) {
            Ok(()) => Answer::Done,
            Err(err) => Answer::Failed(format!("{id} 摘不掉读事件：{err}")),
        }
    }

    fn do_send(&self, ctx: &Ctx, id: SocketId, chunk: Chunk, high: bool) -> Answer {
        let warn_size = self.config.borrow().warn_size;
        let queued = {
            let mut sockets = self.sockets.borrow_mut();
            let Some(socket) = sockets.get_mut(id) else {
                return missing(id);
            };
            if !socket.state.writable() {
                return Answer::Failed(format!("{id} 现在写不了（{}）", socket.state.name()));
            }
            (socket.owner, socket.wb.push(chunk, high, warn_size))
        };
        let (owner, warning) = queued;
        if let Some(kilobytes) = warning {
            let _ = ctx.send(
                owner,
                MsgType::SOCKET,
                Payload::of(SocketEvent::Warning { id, kilobytes }),
            );
        }
        // 能立刻写出去的就别等下一次可写事件
        self.flush(ctx, id);
        Answer::Done
    }

    fn do_udp_send(
        &self,
        ctx: &Ctx,
        id: SocketId,
        to: Option<SocketAddr>,
        data: Vec<u8>,
    ) -> Answer {
        if data.len() > MAX_UDP_PACKAGE {
            return Answer::Failed(format!(
                "UDP 包 {} 字节，超过上限 {MAX_UDP_PACKAGE}",
                data.len()
            ));
        }
        let to = {
            let sockets = self.sockets.borrow();
            let Some(socket) = sockets.get(id) else {
                return missing(id);
            };
            match to.or(socket.udp_peer) {
                Some(to) => to,
                None => {
                    return Answer::Failed(format!("{id} 没有默认对端，得指定地址"));
                }
            }
        };
        self.do_send(ctx, id, Chunk::udp(data, to), true)
    }

    /// 返回 `None` 表示「等写缓冲排空再回话」。
    fn do_close(&self, ctx: &Ctx, id: SocketId, waiting: Pending) -> Option<Answer> {
        // 先尽力把欠的写出去，写完了就地关掉
        self.flush(ctx, id);
        let mut sockets = self.sockets.borrow_mut();
        let Some(socket) = sockets.get_mut(id) else {
            // flush 里已经关掉了（或者压根没这个 socket），都算关上了
            return Some(Answer::Done);
        };
        if socket.wb.is_empty() {
            drop(sockets);
            self.close(ctx, id, Farewell::Closed);
            return Some(Answer::Done);
        }
        // 还欠着数据：转 HALFCLOSE_WRITE，等 flush 把尾巴写完再关并回话
        socket.state = State::HalfCloseWrite;
        socket.pending = Some(waiting);
        let _ = socket.apply(&self.registry);
        None
    }

    fn do_nodelay(&self, id: SocketId, on: bool) -> Answer {
        let sockets = self.sockets.borrow();
        let Some(socket) = sockets.get(id) else {
            return missing(id);
        };
        let Kind::Stream(stream) = &socket.kind else {
            return Answer::Failed(format!("{id} 不是 TCP 连接"));
        };
        match stream.set_nodelay(on) {
            Ok(()) => Answer::Done,
            Err(err) => Answer::Failed(format!("{id} 设 TCP_NODELAY 失败：{err}")),
        }
    }

    fn do_info(&self, id: SocketId) -> Answer {
        match self.sockets.borrow().get(id) {
            Some(socket) => Answer::Info(socket.info()),
            None => missing(id),
        }
    }
}

// ------------------------------------------------------------ 域名解析

impl NetService {
    /// 地址得先解析：把这条命令挪到一个服务内任务里去等。
    ///
    /// 挂起的只是这一个任务，网络层照常收发——这正是内核给
    /// [`ctx.call_external`][Ctx::call_external] 留那条路的用处。
    fn defer(self: &Arc<Self>, ctx: &Ctx, host: String, waiting: Pending, then: Deferred) {
        let me = self.clone();
        let task = ctx.clone();
        ctx.spawn(async move {
            let answer = match me.resolve(&task, host).await {
                Ok(addrs) => me.after_resolve(&task, then, addrs, waiting),
                Err(reason) => Some(Answer::Failed(reason)),
            };
            if let Some(answer) = answer {
                reply(&task, waiting, answer);
            }
        });
    }

    async fn resolve(
        &self,
        ctx: &Ctx,
        host: String,
    ) -> std::result::Result<Vec<SocketAddr>, String> {
        if self.resolver.borrow().is_none() {
            match Resolver::spawn() {
                Ok(resolver) => {
                    self.resolver.replace(Some(resolver));
                }
                Err(err) => return Err(format!("解析线程起不来：{err}")),
            }
        }
        // 闭包在 await 之前就同步跑完，所以这个借用不会跨过挂起点
        let replied = ctx
            .call_external(|token| {
                if let Some(resolver) = self.resolver.borrow().as_ref() {
                    resolver.submit(host, token);
                }
            })
            .await;
        match replied {
            Ok(payload) => match payload.downcast::<Resolved>() {
                Ok(resolved) => resolved.0,
                Err(_) => Err("解析线程回了个认不出的负载".to_string()),
            },
            Err(err) => Err(format!("域名没解析出来：{err}")),
        }
    }

    fn after_resolve(
        &self,
        ctx: &Ctx,
        then: Deferred,
        addrs: Vec<SocketAddr>,
        waiting: Pending,
    ) -> Option<Answer> {
        let Some(&addr) = addrs.first() else {
            return Some(Answer::Failed("解析结果是空的".to_string()));
        };
        match then {
            Deferred::Listen => Some(self.do_listen(addr, waiting.source)),
            Deferred::Connect => self.do_connect_candidates(ctx, addrs.into(), waiting),
            Deferred::Udp => Some(self.do_udp(addr, waiting.source)),
            Deferred::UdpConnect { id } => Some(self.do_udp_connect(id, addr)),
        }
    }
}

/// 回一条应答。发起方压根没等（`send` 而不是 `call`）时什么都不做。
fn reply(ctx: &Ctx, waiting: Pending, answer: Answer) {
    if waiting.is_silent() {
        return;
    }
    let _ = ctx.node().send(
        ctx.handle(),
        waiting.source,
        MsgType::RESPONSE,
        waiting.session,
        Payload::of(answer),
    );
}

fn missing(id: SocketId) -> Answer {
    Answer::Failed(format!("{id} 不存在，可能已经关了"))
}

/// 字面地址就地解析，域名交给解析线程。
fn parse(addr: &str) -> Option<SocketAddr> {
    addr.parse().ok()
}
