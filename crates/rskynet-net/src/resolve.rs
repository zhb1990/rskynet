//! 域名解析线程。C 版没有对应物——skynet 的 socket 层只收 IP，域名由 Lua 那侧
//! 另起服务去查。
//!
//! 这里要一条自己的线程，是因为 [`ToSocketAddrs`] 是阻塞的：在 net 服务那条线程上
//! 直接查，一次慢解析就把所有连接的收发卡住了。
//!
//! 它正好把 [`ext`][rskynet_core::ext] 留的那条路走了一遍：
//!
//! ```text
//! net 服务 ─ ctx.call_external(闭包) ─→ 解析线程 ─ ReplyToken::reply ─→ net 服务醒来
//! ```
//!
//! 于是 net 服务侧写成一句 `.await`，挂起期间它照常处理别的命令与 IO 事件——挂起的
//! 只是「这一次 connect」那个任务。
//!
//! 线程是懒起的：只用 IP 地址的节点压根不会有这条线程。系统 DNS 调用不可取消，
//! 所以服务析构时只关闭任务通道；若线程仍在解析就让它分离，当前解析返回后自行退出，
//! 不能反过来卡住网络服务或节点收工。

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::mpsc::{Sender, channel};
use std::thread::{self, JoinHandle};

use rskynet_core::{Payload, ReplyToken, Result, boxed_payload};

/// 解析结果，经 [`ReplyToken`] 回到 net 服务。
///
/// 失败带原因而不是简单地 `token.fail()`：「连不上」与「域名不存在」对调用方是
/// 两件事，日志里得分得开。
pub(crate) struct Resolved(pub(crate) std::result::Result<Vec<SocketAddr>, String>);

boxed_payload!(Resolved);

struct Job {
    host: String,
    token: ReplyToken,
}

/// 解析线程的把手。
pub(crate) struct Resolver {
    /// 析构时先关掉发送端，让线程办完当前解析后自行退出。
    jobs: Option<Sender<Job>>,
    thread: Option<JoinHandle<()>>,
}

impl Resolver {
    pub(crate) fn spawn() -> Result<Self> {
        let (jobs, inbox) = channel::<Job>();
        let thread = thread::Builder::new()
            .name("rskynet-net-resolve".into())
            .spawn(move || {
                for job in inbox {
                    let resolved = match job.host.to_socket_addrs() {
                        Ok(addrs) => {
                            let addrs: Vec<SocketAddr> = addrs.collect();
                            if addrs.is_empty() {
                                Resolved(Err(format!("`{}` 没解析出任何地址", job.host)))
                            } else {
                                Resolved(Ok(addrs))
                            }
                        }
                        Err(err) => Resolved(Err(format!("解析 `{}` 失败：{err}", job.host))),
                    };
                    // 回执单一旦发出，发起方那句 await 就醒了
                    job.token.reply(Payload::of(resolved));
                }
            })?;
        Ok(Self {
            jobs: Some(jobs),
            thread: Some(thread),
        })
    }

    /// 交一件活过去。线程要是已经没了，回执单跟着丢弃，发起方会拿到错误而不是
    /// 永久挂着（[`ReplyToken`] 的析构会补一个错误回包）。
    pub(crate) fn submit(&self, host: String, token: ReplyToken) {
        if let Some(jobs) = &self.jobs {
            let _ = jobs.send(Job { host, token });
        }
    }
}

impl Drop for Resolver {
    fn drop(&mut self) {
        self.jobs.take();
        if let Some(thread) = self.thread.take() {
            // `to_socket_addrs` 是不可取消的阻塞调用。已经结束时顺手回收；仍在解析时
            // 丢掉 JoinHandle 让它分离，避免 kill net 或节点收工被系统 DNS 卡住。
            // channel 已关闭，当前任务一结束，线程就会从循环退出。
            if thread.is_finished() {
                let _ = thread.join();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn drop_does_not_wait_for_a_blocked_resolver() {
        let (jobs, _inbox) = channel::<Job>();
        let (release, blocked) = channel::<()>();
        let (finished, completion) = channel::<()>();
        let thread = thread::spawn(move || {
            let _ = blocked.recv();
            let _ = finished.send(());
        });
        let resolver = Resolver {
            jobs: Some(jobs),
            thread: Some(thread),
        };

        let started = Instant::now();
        drop(resolver);
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "销毁 resolver 不该等待不可取消的阻塞解析"
        );

        release.send(()).expect("应能放行测试线程");
        completion
            .recv_timeout(Duration::from_secs(1))
            .expect("分离的测试线程应能自行结束");
    }
}
