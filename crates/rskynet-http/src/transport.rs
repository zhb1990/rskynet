use rskynet_core::Ctx;
use rskynet_net::SocketId;

use crate::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum TransportId {
    Plain(SocketId),
    #[cfg(feature = "tls")]
    Tls(rskynet_tls::TlsId),
}

impl TransportId {
    pub(crate) async fn send_wait(self, ctx: &Ctx, data: Vec<u8>) -> Result<()> {
        match self {
            Self::Plain(id) => rskynet_net::send_wait(ctx, id, data)
                .await
                .map_err(Into::into),
            #[cfg(feature = "tls")]
            Self::Tls(id) => rskynet_tls::send_wait(ctx, id, data)
                .await
                .map_err(Into::into),
        }
    }

    pub(crate) async fn pause(self, ctx: &Ctx) -> Result<()> {
        match self {
            Self::Plain(id) => rskynet_net::pause(ctx, id).await.map_err(Into::into),
            #[cfg(feature = "tls")]
            Self::Tls(id) => rskynet_tls::pause(ctx, id).await.map_err(Into::into),
        }
    }

    pub(crate) async fn resume(self, ctx: &Ctx) -> Result<()> {
        match self {
            Self::Plain(id) => rskynet_net::start(ctx, id).await.map_err(Into::into),
            #[cfg(feature = "tls")]
            Self::Tls(id) => rskynet_tls::start(ctx, id).await.map_err(Into::into),
        }
    }

    pub(crate) fn shutdown(self, ctx: &Ctx) {
        match self {
            Self::Plain(id) => {
                let _ = rskynet_net::shutdown(ctx, id);
            }
            #[cfg(feature = "tls")]
            Self::Tls(id) => {
                let _ = rskynet_tls::shutdown(ctx, id);
            }
        }
    }
}
