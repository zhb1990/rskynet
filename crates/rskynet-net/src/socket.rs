//! 槽位表与每个 socket 的状态，对照 `socket_server.c` 的 `struct socket` 与
//! `HASH_ID`。
//!
//! # 为什么照抄 HASH_ID 而不用 slab
//!
//! id 单调递增、槽位是 `id % max_socket`，于是**槽位号可以直接当 mio 的
//! `Token`**：从事件拿到 token 就等于拿到了槽位，一次数组下标就找到 socket，不必
//! 再维护一张 `Token -> id` 的表。同一个槽位被复用时 id 必然变了，所以取用时校验
//! 一句 `slot.id == id` 就能把迟到的命令挡在外面——slab 那套「下标 + 版本号」解决
//! 的是同一个问题，而这里的版本号是免费的（id 自己就是）。

use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;

use mio::event::Source;
use mio::net::{TcpListener, TcpStream, UdpSocket};
use mio::{Interest, Registry, Token};

use crate::buffer::{ReadSizer, WriteQueue};
use crate::event::{SocketId, SocketInfo};

/// 内核用来叫醒 `poll` 的 token，与任何槽位都撞不上。
pub(crate) const WAKE: Token = Token(usize::MAX);

/// 承载物。三种东西的事件处理方式不同，但槽位、写缓冲、状态机是共用的。
pub(crate) enum Kind {
    Listener(TcpListener),
    Stream(TcpStream),
    Udp(UdpSocket),
}

impl Kind {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Kind::Listener(_) => "listener",
            Kind::Stream(_) => "stream",
            Kind::Udp(_) => "udp",
        }
    }

    /// 注册进 poll 时要交给 mio 的那个东西。
    fn source(&mut self) -> &mut dyn Source {
        match self {
            Kind::Listener(listener) => listener,
            Kind::Stream(stream) => stream,
            Kind::Udp(socket) => socket,
        }
    }

    fn local_addr(&self) -> Option<SocketAddr> {
        match self {
            Kind::Listener(listener) => listener.local_addr().ok(),
            Kind::Stream(stream) => stream.local_addr().ok(),
            Kind::Udp(socket) => socket.local_addr().ok(),
        }
    }
}

/// 状态机，与 C 版的 `SOCKET_TYPE_*` 一一对应。
///
/// 「暂停读」在 C 版里不是一个状态（它只是把 `EPOLLIN` 摘掉），这里也一样，用
/// [`Socket::paused`] 那个标志表示。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum State {
    /// 已经 bind 好，还没进 poll，等属主调 `start`。对照 `SOCKET_TYPE_PLISTEN`。
    PreListen,
    /// 正在 accept。对照 `SOCKET_TYPE_LISTEN`。
    Listen,
    /// `connect` 已发出，等可写事件宣布结果。对照 `SOCKET_TYPE_CONNECTING`。
    Connecting,
    /// accept 出来了，还没进 poll，等属主调 `start`。对照 `SOCKET_TYPE_PACCEPT`。
    ///
    /// 这个空档是有意留的：连接刚建立时属主往往是监听服务，它要先把连接转交给
    /// 干活的那个服务，转交完由新属主 `start`，第一个字节才不会投错人。
    PreAccept,
    /// 正常收发。对照 `SOCKET_TYPE_CONNECTED`。
    Connected,
    /// 对端关了写，我们读不到东西了，但写缓冲还没排空。对照
    /// `SOCKET_TYPE_HALFCLOSE_READ`。
    HalfCloseRead,
    /// 属主要求关闭，等写缓冲排空。对照 `SOCKET_TYPE_HALFCLOSE_WRITE`。
    HalfCloseWrite,
}

impl State {
    pub(crate) fn name(self) -> &'static str {
        match self {
            State::PreListen => "prelisten",
            State::Listen => "listen",
            State::Connecting => "connecting",
            State::PreAccept => "preaccept",
            State::Connected => "connected",
            State::HalfCloseRead => "halfclose_read",
            State::HalfCloseWrite => "halfclose_write",
        }
    }

    /// 能不能往它上面写。
    pub(crate) fn writable(self) -> bool {
        matches!(self, State::Connected | State::HalfCloseRead)
    }
}

/// 一次「等事情办完才回话」的凭据：`connect` 要等连上，`close` 要等写完。
#[derive(Debug, Clone, Copy)]
pub(crate) struct Pending {
    pub(crate) source: rskynet_core::Handle,
    pub(crate) session: u64,
}

impl Pending {
    /// 发起方压根不等回话（用 `send` 而不是 `call` 下的命令）。
    pub(crate) fn is_silent(&self) -> bool {
        self.session == 0
    }
}

#[derive(Default)]
pub(crate) struct Stat {
    pub(crate) accept_count: u64,
    pub(crate) read_bytes: u64,
    pub(crate) write_bytes: u64,
    pub(crate) last_read_at_ms: Option<u64>,
    pub(crate) last_write_at_ms: Option<u64>,
}

pub(crate) struct Socket {
    pub(crate) id: SocketId,
    /// 槽位号，也就是它在 poll 里的 token。
    slot: usize,
    /// 属主服务的 handle，事件投给它。对照 C 版的 `opaque`。
    pub(crate) owner: rskynet_core::Handle,
    pub(crate) kind: Kind,
    pub(crate) state: State,
    /// 读被 [`pause`][crate::pause] 掐了。
    pub(crate) paused: bool,
    /// 当前注册进 poll 的兴趣；`None` 表示压根没在 poll 里。
    interest: Option<Interest>,
    pub(crate) wb: WriteQueue,
    pub(crate) read: ReadSizer,
    /// UDP 的默认对端，[`udp_connect`][crate::udp_connect] 设的。
    pub(crate) udp_peer: Option<SocketAddr>,
    /// 还在等回话的那次命令。
    pub(crate) pending: Option<Pending>,
    /// 等写队列降到低水位的 `send_wait` 调用者。
    pub(crate) send_waiters: VecDeque<Pending>,
    /// 域名解析出的其余地址。当前连接失败时按顺序继续尝试。
    pub(crate) connect_fallbacks: VecDeque<SocketAddr>,
    /// 整次建连的绝对截止时刻；切换到解析出的后备地址时继续沿用。
    pub(crate) connect_deadline_ms: Option<u64>,
    pub(crate) stat: Stat,
}

impl Socket {
    fn new(
        id: SocketId,
        slot: usize,
        owner: rskynet_core::Handle,
        kind: Kind,
        state: State,
        floor: usize,
    ) -> Self {
        Self {
            id,
            slot,
            owner,
            kind,
            state,
            paused: false,
            interest: None,
            wb: WriteQueue::default(),
            read: ReadSizer::new(floor),
            udp_peer: None,
            pending: None,
            send_waiters: VecDeque::new(),
            connect_fallbacks: VecDeque::new(),
            connect_deadline_ms: None,
            stat: Stat::default(),
        }
    }

    pub(crate) fn token(&self) -> Token {
        Token(self.slot)
    }

    /// 按当前状态算出该向 poll 要什么，然后把差额落实下去。
    ///
    /// 每次状态变化都调它，于是「什么时候该 register、什么时候该 reregister、
    /// 什么时候该 deregister」只有这一处需要想明白。mio 的 `Interest` 不能为空，
    /// 所以「什么都不关心」只能表达成注销——暂停读且没有待写数据时就是这种情况。
    pub(crate) fn apply(&mut self, registry: &Registry) -> io::Result<()> {
        let want = self.desired();
        let token = self.token();
        match (want, self.interest) {
            (Some(want), None) => registry.register(self.kind.source(), token, want)?,
            (Some(want), Some(have)) if want != have => {
                registry.reregister(self.kind.source(), token, want)?
            }
            (Some(_), Some(_)) => {}
            (None, Some(_)) => registry.deregister(self.kind.source())?,
            (None, None) => {}
        }
        self.interest = want;
        Ok(())
    }

    /// 从 poll 里摘掉，销毁前调。
    pub(crate) fn detach(&mut self, registry: &Registry) {
        if self.interest.take().is_some() {
            let _ = registry.deregister(self.kind.source());
        }
    }

    /// 该向 poll 要什么。
    ///
    /// 注意 `paused` **不在**这里体现：暂停是「不去读」，不是「不听」。mio 是边沿
    /// 触发的，不读就不会再来事件，所以留着 READABLE 不花什么钱；而摘掉它意味着
    /// 注销（`Interest` 不能为空），偏偏「注销之后再注册」在 Windows 上不可靠。
    /// 恢复后的补读由 [`crate::service::NetService`] 负责。
    fn desired(&self) -> Option<Interest> {
        match self.state {
            // 还没 start：压根没进过 poll
            State::PreListen | State::PreAccept => None,
            State::Listen => Some(Interest::READABLE),
            // 连上没连上，靠可写事件宣布
            State::Connecting => Some(Interest::WRITABLE),
            State::Connected if self.wb.is_empty() => Some(Interest::READABLE),
            State::Connected => Some(Interest::READABLE.add(Interest::WRITABLE)),
            // 读那一半已经没意义了，只剩把欠的写完
            State::HalfCloseRead | State::HalfCloseWrite => Some(Interest::WRITABLE),
        }
    }

    pub(crate) fn info(&self) -> SocketInfo {
        let peer = match &self.kind {
            Kind::Stream(stream) => stream.peer_addr().ok(),
            _ => self.udp_peer,
        };
        SocketInfo {
            id: self.id,
            owner: self.owner,
            owner_kind: None,
            owner_names: Vec::new(),
            kind: self.kind.name(),
            state: self.state.name(),
            paused: self.paused,
            local: self.kind.local_addr(),
            peer,
            write_pending: self.wb.size(),
            accept_count: self.stat.accept_count,
            read_bytes: self.stat.read_bytes,
            write_bytes: self.stat.write_bytes,
            last_read_at_ms: self.stat.last_read_at_ms,
            last_write_at_ms: self.stat.last_write_at_ms,
            reading: !self.paused && matches!(self.state, State::Listen | State::Connected),
            writing: !self.wb.is_empty(),
        }
    }
}

/// 槽位表，对照 C 版 `socket_server` 里那个定长的 `slot` 数组。
pub(crate) struct Sockets {
    slots: Vec<Option<Socket>>,
    /// 下一个要发的 id。从 1 起，0 留作「没有」。
    next: u32,
    /// 新建 socket 的读缓冲从多大开始。
    floor: usize,
}

impl Sockets {
    /// 表在 `init` 里才建起来：容量是配置项，而服务对象在读配置之前就存在了。
    pub(crate) fn new(capacity: usize, floor: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        slots.resize_with(capacity, || None);
        Self {
            slots,
            next: 1,
            floor,
        }
    }

    /// 占一个槽位并造一个 socket，槽位满了返回 `None`。
    ///
    /// 找法与 C 版一样：从 `next` 起往后扫一圈，谁空用谁。扫满一圈还没有就是真满了。
    pub(crate) fn insert(
        &mut self,
        owner: rskynet_core::Handle,
        kind: Kind,
        state: State,
    ) -> Option<&mut Socket> {
        let capacity = self.slots.len();
        for _ in 0..capacity {
            let id = self.next;
            // id 跳过 0：0 在别处兼作「没有这个 socket」
            self.next = self.next.wrapping_add(1);
            if self.next == 0 {
                self.next = 1;
            }
            let slot = id as usize % capacity;
            if self.slots[slot].is_some() {
                continue;
            }
            let socket = Socket::new(SocketId(id), slot, owner, kind, state, self.floor);
            return Some(self.slots[slot].insert(socket));
        }
        None
    }

    /// 按 id 取。校验槽位里那个的 id 对不对得上，于是迟到的命令找不到人。
    pub(crate) fn get(&self, id: SocketId) -> Option<&Socket> {
        let slot = self.slots.get(id.0 as usize % self.slots.len())?;
        slot.as_ref().filter(|socket| socket.id == id)
    }

    pub(crate) fn get_mut(&mut self, id: SocketId) -> Option<&mut Socket> {
        let len = self.slots.len();
        let slot = self.slots.get_mut(id.0 as usize % len)?;
        slot.as_mut().filter(|socket| socket.id == id)
    }

    /// 按 poll 事件里的 token 取，也就是直接按槽位号取。
    pub(crate) fn by_token(&self, token: Token) -> Option<&Socket> {
        self.slots.get(token.0)?.as_ref()
    }

    pub(crate) fn remove(&mut self, id: SocketId) -> Option<Socket> {
        let len = self.slots.len();
        let slot = self.slots.get_mut(id.0 as usize % len)?;
        if slot.as_ref().is_some_and(|socket| socket.id == id) {
            slot.take()
        } else {
            None
        }
    }

    /// 枚举当前仍占着槽位的全部 socket，按 id 排序以提供稳定输出。
    pub(crate) fn infos(&self) -> Vec<SocketInfo> {
        let mut infos: Vec<_> = self
            .slots
            .iter()
            .filter_map(|slot| slot.as_ref().map(Socket::info))
            .collect();
        infos.sort_unstable_by_key(|info| info.id);
        infos
    }
}

impl Default for Sockets {
    /// 空表。真正的容量由 `init` 读完配置再给。
    fn default() -> Self {
        Self {
            slots: Vec::new(),
            next: 1,
            floor: 64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn listener() -> Kind {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).expect("应能绑定");
        Kind::Listener(listener)
    }

    /// id 落槽的规则要与 HASH_ID 一致，取用时按 id 校验
    #[test]
    fn ids_hash_into_slots() {
        let mut sockets = Sockets::new(4, 64);
        let first = sockets
            .insert(1, listener(), State::PreListen)
            .expect("应有空槽")
            .id;
        assert_eq!(first, SocketId(1));
        assert_eq!(sockets.get(first).map(|s| s.token()), Some(Token(1)));

        // 槽位号就是 token，也就是 id % 容量
        let ids: Vec<SocketId> = (0..3)
            .map(|_| {
                sockets
                    .insert(1, listener(), State::PreListen)
                    .expect("应有空槽")
                    .id
            })
            .collect();
        assert_eq!(ids, vec![SocketId(2), SocketId(3), SocketId(4)]);
        assert_eq!(sockets.get(SocketId(4)).unwrap().token(), Token(0));

        // 满了就是满了
        assert!(sockets.insert(1, listener(), State::PreListen).is_none());
    }

    /// 槽位复用之后，拿旧 id 来找是找不到人的
    #[test]
    fn a_recycled_slot_rejects_the_old_id() {
        let mut sockets = Sockets::new(4, 64);
        for _ in 0..4 {
            sockets.insert(1, listener(), State::PreListen);
        }
        let stale = SocketId(1);
        sockets.remove(stale).expect("应当摘得掉");
        let fresh = sockets
            .insert(1, listener(), State::PreListen)
            .expect("腾出来了")
            .id;

        // 新旧 id 落在同一个槽位，但只有新的认得出来
        assert_eq!(stale.0 as usize % 4, fresh.0 as usize % 4);
        assert!(sockets.get(stale).is_none(), "旧 id 不该再找得到人");
        assert!(sockets.get(fresh).is_some());
    }

    /// 该向 poll 要什么，只由状态与写缓冲决定
    #[test]
    fn interest_follows_the_state() {
        let mut sockets = Sockets::new(4, 64);
        let socket = sockets.insert(1, listener(), State::PreListen).unwrap();
        assert_eq!(socket.desired(), None, "没 start 之前不进 poll");

        socket.state = State::Listen;
        assert_eq!(socket.desired(), Some(Interest::READABLE));

        socket.state = State::Connected;
        assert_eq!(socket.desired(), Some(Interest::READABLE));

        // 暂停读不改变注册：不读就不会再来事件，而注销之后再注册在 Windows 上
        // 不保证能用
        socket.paused = true;
        assert_eq!(socket.desired(), Some(Interest::READABLE));
        socket.paused = false;

        socket
            .wb
            .push(crate::buffer::Chunk::tcp(vec![1]), true, usize::MAX);
        assert_eq!(
            socket.desired(),
            Some(Interest::READABLE.add(Interest::WRITABLE))
        );

        socket.state = State::HalfCloseWrite;
        assert_eq!(socket.desired(), Some(Interest::WRITABLE), "只剩把欠的写完");
    }

    #[test]
    fn infos_include_transitional_states_and_are_sorted() {
        let mut sockets = Sockets::new(4, 64);
        // id 3 落在槽 3，id 4 落在槽 0；直接按槽遍历会得到 [4, 3]。
        sockets.next = 3;
        let low = sockets
            .insert(3, listener(), State::PreListen)
            .expect("应能插入")
            .id;
        let high = sockets
            .insert(2, listener(), State::Listen)
            .expect("应能插入")
            .id;

        let infos = sockets.infos();
        assert_eq!(
            infos.iter().map(|info| info.id).collect::<Vec<_>>(),
            vec![low, high]
        );
        assert_eq!(infos[0].state, "prelisten");
        assert_eq!(infos[1].state, "listen");

        sockets.remove(high);
        assert_eq!(sockets.infos().len(), 1, "释放后的 socket 不应再显示");
    }
}
