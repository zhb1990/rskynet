//! rskynet 的 HTTP/1.1 支持：共享客户端服务与可嵌入业务 Service 的服务端驱动。

mod body;
pub mod client;
mod config;
mod error;
mod server;
mod transport;

use rskynet_core::Registry;

pub use body::{BodySpec, IncomingBody, OutgoingBody};
pub use client::{ClientExchange, HttpClientService};
pub use config::{HttpClientConfig, HttpServerConfig};
pub use error::HttpError;
pub use server::{HttpListenerId, HttpServer, ServerRequest, ServerResponder};
pub use ureq_proto::http;

pub type Result<T> = std::result::Result<T, HttpError>;
pub const NAME: &str = client::NAME;

pub trait RegistryExt {
    #[must_use]
    fn with_http_client(self) -> Self;
}

impl RegistryExt for Registry {
    fn with_http_client(self) -> Self {
        self.with(NAME, HttpClientService::new)
    }
}
