//! rskynet 的通用 QUIC 传输层。
//!
//! 本 crate 直接驱动 `quinn-proto` 的 sans-I/O 状态机，UDP 收发仍由
//! `rskynet-net` 的独占 reactor 完成，不需要 Tokio。

mod command;
mod config;
mod event;
mod service;

use rskynet_core::Registry;

pub use command::{
    Answer, Command, QuicClientOptions, QuicServerOptions, close, close_listener, connect, finish,
    info, listen, local_addr, netstat, open_bi, open_uni, reset, send, send_datagram, send_wait,
    shutdown, start, stop,
};
pub use config::{QuicConfig, QuicTransportOptions};
pub use event::{
    QuicConnectionId, QuicEvent, QuicInfo, QuicListenerId, QuicStreamId, StreamDirection,
};
pub use rskynet_tls::{
    CertificateDer, ClientTlsConfig, PrivateKeyDer, PrivateKeyInput, SecretBytes, ServerName,
    ServerTlsConfig, ServerVerification, TlsConfigError, certificates_from_pem,
};
pub use service::QuicService;

pub const NAME: &str = "quic";

pub trait RegistryExt {
    #[must_use]
    fn with_quic(self) -> Self;
}

impl RegistryExt for Registry {
    fn with_quic(self) -> Self {
        self.with(NAME, QuicService::new)
    }
}
