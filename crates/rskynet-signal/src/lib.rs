//! 进程信号服务与崩溃报告支持。

pub mod crash;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock, mpsc};
use std::time::Duration;

use rskynet_core::{Ctx, Error, Idler, Result, SvcCell};

/// 可以由 [`rskynet_macros::signal`] 注册的普通进程信号。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Signal {
    Interrupt,
    Terminate,
    Hangup,
    Quit,
    User1,
    User2,
}

/// 链接期信号回调描述，由 `#[rskynet::signal(..)]` 生成。
pub struct AutoSignal {
    pub signal: Signal,
    pub source: &'static str,
    callback: fn(&Ctx),
}

impl AutoSignal {
    #[doc(hidden)]
    pub const fn new(signal: Signal, source: &'static str, callback: fn(&Ctx)) -> Self {
        Self {
            signal,
            source,
            callback,
        }
    }

    fn call(&self, ctx: &Ctx) {
        (self.callback)(ctx);
    }
}

inventory::collect!(AutoSignal);

type Subscriber = (usize, mpsc::Sender<Signal>);
static SUBSCRIBERS: OnceLock<Mutex<Vec<Subscriber>>> = OnceLock::new();
static BACKEND: OnceLock<std::result::Result<(), String>> = OnceLock::new();
static NEXT_SUBSCRIBER: AtomicUsize = AtomicUsize::new(1);

fn subscribers() -> &'static Mutex<Vec<Subscriber>> {
    SUBSCRIBERS.get_or_init(|| Mutex::new(Vec::new()))
}

fn broadcast(signal: Signal) {
    let mut subscribers = subscribers().lock().unwrap_or_else(|err| err.into_inner());
    subscribers.retain(|(_, sender)| sender.send(signal).is_ok());
}

fn subscribe() -> Result<Subscription> {
    match BACKEND.get_or_init(start_backend) {
        Ok(()) => {}
        Err(reason) => return Err(Error::service(reason.clone())),
    }
    let (sender, receiver) = mpsc::channel();
    let id = NEXT_SUBSCRIBER.fetch_add(1, Ordering::Relaxed);
    subscribers()
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .push((id, sender));
    Ok(Subscription { id, receiver })
}

struct Subscription {
    id: usize,
    receiver: mpsc::Receiver<Signal>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        subscribers()
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .retain(|(id, _)| *id != self.id);
    }
}

#[cfg(unix)]
fn start_backend() -> std::result::Result<(), String> {
    use signal_hook::consts::signal::{SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGUSR1, SIGUSR2};

    let mut raw = vec![SIGINT, SIGTERM];
    for registration in inventory::iter::<AutoSignal> {
        let value = match registration.signal {
            Signal::Interrupt => SIGINT,
            Signal::Terminate => SIGTERM,
            Signal::Hangup => SIGHUP,
            Signal::Quit => SIGQUIT,
            Signal::User1 => SIGUSR1,
            Signal::User2 => SIGUSR2,
        };
        if !raw.contains(&value) {
            raw.push(value);
        }
    }
    let mut signals = signal_hook::iterator::Signals::new(raw)
        .map_err(|err| format!("安装进程信号处理器失败：{err}"))?;
    std::thread::Builder::new()
        .name("rskynet-signal-source".into())
        .spawn(move || {
            for value in signals.forever() {
                let signal = match value {
                    SIGINT => Signal::Interrupt,
                    SIGTERM => Signal::Terminate,
                    SIGHUP => Signal::Hangup,
                    SIGQUIT => Signal::Quit,
                    SIGUSR1 => Signal::User1,
                    SIGUSR2 => Signal::User2,
                    _ => continue,
                };
                broadcast(signal);
            }
        })
        .map_err(|err| format!("创建进程信号线程失败：{err}"))?;
    Ok(())
}

#[cfg(windows)]
fn start_backend() -> std::result::Result<(), String> {
    ctrlc::set_handler(|| broadcast(Signal::Interrupt))
        .map_err(|err| format!("安装 Ctrl+C 处理器失败：{err}"))
}

#[cfg(not(any(unix, windows)))]
fn start_backend() -> std::result::Result<(), String> {
    Err("当前平台不支持进程信号".into())
}

/// 接收 OS 信号并在自己的线程上调用业务回调。
#[derive(Default)]
pub struct SignalService {
    subscription: SvcCell<Option<Subscription>>,
}

#[rskynet_macros::exclusive(crate = ::rskynet_core, name = "signal")]
impl SignalService {
    async fn init(&self) -> Result<()> {
        self.subscription.replace(Some(subscribe()?));
        Ok(())
    }

    fn idle(&self, ctx: &Ctx, _idler: &Idler) {
        let received = self
            .subscription
            .borrow()
            .as_ref()
            .and_then(|subscription| {
                subscription
                    .receiver
                    .recv_timeout(Duration::from_millis(100))
                    .ok()
            });
        if let Some(signal) = received {
            dispatch(ctx, signal);
        }
    }
}

fn dispatch(ctx: &Ctx, signal: Signal) {
    if let Some(registration) = registration(signal) {
        registration.call(ctx);
    } else if has_default_shutdown(signal) {
        ctx.log(format!("收到进程信号 {signal:?}，执行默认 abort"));
        ctx.abort();
    }
}

fn registration(signal: Signal) -> Option<&'static AutoSignal> {
    inventory::iter::<AutoSignal>
        .into_iter()
        .find(|registration| registration.signal == signal)
}

fn has_default_shutdown(signal: Signal) -> bool {
    matches!(signal, Signal::Interrupt | Signal::Terminate) && registration(signal).is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supported_signal_values_are_distinct() {
        let values = [
            Signal::Interrupt,
            Signal::Terminate,
            Signal::Hangup,
            Signal::Quit,
            Signal::User1,
            Signal::User2,
        ];
        for (index, value) in values.iter().enumerate() {
            assert!(!values[..index].contains(value));
        }
    }

    #[test]
    fn interrupt_and_terminate_default_to_shutdown() {
        assert!(has_default_shutdown(Signal::Interrupt));
        assert!(has_default_shutdown(Signal::Terminate));
        assert!(!has_default_shutdown(Signal::Hangup));
        assert!(!has_default_shutdown(Signal::Quit));
    }
}
