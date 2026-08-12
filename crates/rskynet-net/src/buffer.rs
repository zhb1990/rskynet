//! 写缓冲与读缓冲，对照 `socket_server.c` 的 `wb_list` 与 `p.size` 那套自适应。
//!
//! 两件事都是「一个连接慢下来时不要拖垮别人」：
//!
//! - **写缓冲**：内核缓冲写满时余下的字节先攒着，等 `EPOLLOUT` 再续写。攒的量
//!   越过阈值就给属主报警（[`WriteQueue::push`] 的返回值），限流与踢人的决定权
//!   交给属主，网络层自己不丢数据。
//! - **读缓冲**：一次读满就把下次的缓冲翻倍，读得少就减半。省的是「小包连接
//!   各占一个大缓冲」的内存，以及「大流量连接每次只读一小口」的系统调用。
//!
//! 与 C 版的差别有两处。一是这里只收 `Vec<u8>`，没有 C 版 `send_object` 那层
//! 「用户对象 + 取指针回调」的间接——同进程传对象在 rskynet 里走
//! [`Payload::Boxed`][rskynet_core::Payload]，压根不必经过网络层。二是队列排空时
//! 顺手把告警阈值复位，于是下一波堆积还会再报一次；C 版的 `warn_size` 只增不减，
//! 报过一次之后就再也不吭声了。

use std::collections::VecDeque;
use std::net::SocketAddr;

/// 一段待写的数据。
pub(crate) struct Chunk {
    data: Vec<u8>,
    /// 已经写出去多少。C 版是 `ptr` 往前挪，这里记偏移。
    offset: usize,
    /// UDP 才有：这一包发给谁。TCP 是 `None`。
    to: Option<SocketAddr>,
}

impl Chunk {
    pub(crate) fn tcp(data: Vec<u8>) -> Self {
        Self {
            data,
            offset: 0,
            to: None,
        }
    }

    pub(crate) fn udp(data: Vec<u8>, to: SocketAddr) -> Self {
        Self {
            data,
            offset: 0,
            to: Some(to),
        }
    }

    /// 还没写出去的那一截。
    pub(crate) fn rest(&self) -> &[u8] {
        &self.data[self.offset..]
    }

    pub(crate) fn to(&self) -> Option<SocketAddr> {
        self.to
    }
}

/// 一个连接的写缓冲：高低两条队列，对照 C 版的 `high` 与 `low`。
///
/// 分两条是为了让「必须先到」的数据（握手、心跳、踢人通知）不被大块数据堵在
/// 后面：高优先队列排空了才从低优先里提一段上来。
#[derive(Default)]
pub(crate) struct WriteQueue {
    high: VecDeque<Chunk>,
    low: VecDeque<Chunk>,
    /// 两条队列里还压着多少字节，对照 C 版的 `wb_size`。
    size: usize,
    /// 下一次该在堆到多少字节时报警，对照 C 版的 `warn_size`。
    warn_at: usize,
}

impl WriteQueue {
    /// 压一段进来。返回 `Some(千字节)` 表示该给属主报一次警了。
    ///
    /// 报警的节奏照抄 C 版：第一次在越过 `threshold` 时报，之后阈值翻倍，于是
    /// 1 MiB、2 MiB、4 MiB 各报一次，不会每压一段就吵一遍。
    pub(crate) fn push(&mut self, chunk: Chunk, high: bool, threshold: usize) -> Option<usize> {
        self.size += chunk.rest().len();
        if high {
            self.high.push_back(chunk);
        } else {
            self.low.push_back(chunk);
        }
        if self.size < threshold || self.size < self.warn_at {
            return None;
        }
        self.warn_at = if self.warn_at == 0 {
            threshold * 2
        } else {
            self.warn_at * 2
        };
        Some(self.size / 1024)
    }

    /// 下一段该写的数据。高优先空了就从低优先提一段上来，对照 C 版的
    /// `raise_uncomplete`。
    pub(crate) fn next(&mut self) -> Option<&Chunk> {
        if self.high.is_empty() {
            if let Some(chunk) = self.low.pop_front() {
                self.high.push_back(chunk);
            }
        }
        self.high.front()
    }

    /// 记下刚写出去 `written` 字节。写完一整段就把它弹掉。
    pub(crate) fn consume(&mut self, written: usize) {
        self.size -= written;
        let Some(chunk) = self.high.front_mut() else {
            return;
        };
        chunk.offset += written;
        if chunk.offset >= chunk.data.len() {
            self.high.pop_front();
        }
        if self.is_empty() {
            // 排空即复位：下一波堆积仍然值得报一次，而不是从此闭嘴
            self.warn_at = 0;
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.high.is_empty() && self.low.is_empty()
    }

    pub(crate) fn size(&self) -> usize {
        self.size
    }
}

/// 读缓冲该开多大，对照 C 版 `socket` 结构里的 `p.size`。
pub(crate) struct ReadSizer {
    size: usize,
    /// 下限，也是起始值，对照 C 版 `MIN_READ_BUFFER`。
    floor: usize,
}

impl ReadSizer {
    pub(crate) fn new(floor: usize) -> Self {
        Self { size: floor, floor }
    }

    pub(crate) fn size(&self) -> usize {
        self.size
    }

    /// 按这次实际读到的量调整下次的缓冲，规则与 C 版一字不差：读满了就翻倍，
    /// 读到的不足一半就减半（但不低于下限）。
    pub(crate) fn observe(&mut self, read: usize) {
        if read == self.size {
            self.size *= 2;
        } else if self.size > self.floor && read * 2 < self.size {
            self.size /= 2;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(len: usize) -> Chunk {
        Chunk::tcp(vec![0u8; len])
    }

    /// 高优先的先走；高优先空了才从低优先提一段上来
    #[test]
    fn high_priority_drains_first() {
        let mut queue = WriteQueue::default();
        queue.push(Chunk::tcp(b"low".to_vec()), false, usize::MAX);
        queue.push(Chunk::tcp(b"high".to_vec()), true, usize::MAX);
        assert_eq!(queue.size(), 7);

        assert_eq!(queue.next().unwrap().rest(), b"high");
        queue.consume(4);
        assert_eq!(queue.next().unwrap().rest(), b"low");
        queue.consume(3);
        assert!(queue.is_empty());
        assert_eq!(queue.size(), 0);
    }

    /// 一段没写完就接着写剩下的那一截
    #[test]
    fn a_partial_write_resumes_where_it_stopped() {
        let mut queue = WriteQueue::default();
        queue.push(Chunk::tcp(b"hello".to_vec()), true, usize::MAX);
        assert_eq!(queue.next().unwrap().rest(), b"hello");
        queue.consume(2);
        assert_eq!(queue.next().unwrap().rest(), b"llo");
        assert_eq!(queue.size(), 3);
        queue.consume(3);
        assert!(queue.is_empty());
    }

    /// 告警在越过阈值时报一次，之后每翻倍再报一次，不会每压一段就吵
    #[test]
    fn warnings_double_the_threshold() {
        let mut queue = WriteQueue::default();
        assert_eq!(queue.push(chunk(600), true, 1024), None, "还没到阈值");
        assert_eq!(
            queue.push(chunk(600), true, 1024),
            Some(1),
            "越过 1024 报一次"
        );
        assert_eq!(queue.push(chunk(600), true, 1024), None, "还没翻倍，不吵");
        assert_eq!(queue.push(chunk(1000), true, 1024), Some(2), "过 2048 再报");

        // 排空之后阈值复位，下一波堆积还值得报
        queue.consume(600);
        queue.consume(600);
        queue.consume(600);
        queue.consume(1000);
        assert!(queue.is_empty());
        assert_eq!(queue.push(chunk(2000), true, 1024), Some(1));
    }

    /// 读满就翻倍，读得少就减半，但不低于下限
    #[test]
    fn read_buffer_follows_the_traffic() {
        let mut sizer = ReadSizer::new(64);
        sizer.observe(64);
        assert_eq!(sizer.size(), 128);
        sizer.observe(128);
        assert_eq!(sizer.size(), 256);

        // 读到的不足一半 → 减半
        sizer.observe(10);
        assert_eq!(sizer.size(), 128);
        // 刚好一半 → 不动
        sizer.observe(64);
        assert_eq!(sizer.size(), 128);

        for _ in 0..10 {
            sizer.observe(0);
        }
        assert_eq!(sizer.size(), 64, "不该跌破下限");
    }
}
