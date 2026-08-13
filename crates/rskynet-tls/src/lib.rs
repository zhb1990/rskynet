//! rskynet 的 TLS 协议服务。
//!
//! 本 crate 不建立 TCP 连接，也不轮询 fd；底层密文完全通过 `rskynet-net` 收发。
//! 业务服务只看见 [`TlsId`] 和 [`TlsEvent`] 中的明文。

mod command;
mod config;
mod event;
mod identity;
mod service;

use rskynet_core::Registry;

pub use command::{
    Answer, ClientOptions, Command, ServerOptions, close, connect, info, listen, pause, send,
    send_low, send_low_wait, send_wait, shutdown, start,
};
pub use config::TlsConfig;
pub use event::{TlsEvent, TlsId, TlsInfo};
pub use identity::{
    ClientTlsConfig, PrivateKeyInput, SecretBytes, ServerTlsConfig, ServerVerification,
    TlsConfigError, certificates_from_pem,
};
pub use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
pub use service::TlsService;

pub const NAME: &str = "tls";

pub trait RegistryExt {
    #[must_use]
    fn with_tls(self) -> Self;
}

impl RegistryExt for Registry {
    fn with_tls(self) -> Self {
        self.with(NAME, TlsService::new)
    }
}
