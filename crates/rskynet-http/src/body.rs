use rskynet_core::Ctx;

use crate::{HttpError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodySpec {
    Empty,
    Fixed(u64),
    Chunked,
}

pub(crate) enum BodyDriver {
    Client(crate::client::ClientBodyHandle),
    Server {
        handle: crate::server::ServerBodyHandle,
        active: bool,
    },
}

pub struct IncomingBody {
    pub(crate) driver: Option<BodyDriver>,
}

impl std::fmt::Debug for IncomingBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncomingBody").finish_non_exhaustive()
    }
}

impl IncomingBody {
    pub(crate) fn client(handle: crate::client::ClientBodyHandle) -> Self {
        Self {
            driver: Some(BodyDriver::Client(handle)),
        }
    }

    pub(crate) fn server(handle: crate::server::ServerBodyHandle) -> Self {
        Self {
            driver: Some(BodyDriver::Server {
                handle,
                active: true,
            }),
        }
    }

    pub async fn next_chunk(&mut self, ctx: &Ctx) -> Result<Option<Vec<u8>>> {
        match self.driver.as_mut().ok_or(HttpError::BodyClosed)? {
            BodyDriver::Client(handle) => handle.next_chunk(ctx).await,
            BodyDriver::Server { handle, active } => {
                let result = handle.next_chunk(ctx).await?;
                if result.is_none() {
                    *active = false;
                }
                Ok(result)
            }
        }
    }

    pub async fn discard(mut self, ctx: &Ctx) -> Result<()> {
        while self.next_chunk(ctx).await?.is_some() {}
        self.driver.take();
        Ok(())
    }

    pub async fn collect(mut self, ctx: &Ctx, limit: usize) -> Result<Vec<u8>> {
        let mut all = Vec::new();
        while let Some(chunk) = self.next_chunk(ctx).await? {
            if all.len().saturating_add(chunk.len()) > limit {
                return Err(HttpError::BackpressureLimit {
                    actual: all.len() + chunk.len(),
                    limit,
                });
            }
            all.extend_from_slice(&chunk);
        }
        self.driver.take();
        Ok(all)
    }
}

pub(crate) enum OutgoingDriver {
    Server {
        handle: crate::server::ServerBodyHandle,
        active: bool,
    },
}

pub struct OutgoingBody {
    pub(crate) driver: Option<OutgoingDriver>,
}

impl std::fmt::Debug for OutgoingBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutgoingBody").finish_non_exhaustive()
    }
}

impl OutgoingBody {
    pub(crate) fn server(handle: crate::server::ServerBodyHandle) -> Self {
        Self {
            driver: Some(OutgoingDriver::Server {
                handle,
                active: true,
            }),
        }
    }

    pub(crate) fn finished() -> Self {
        Self { driver: None }
    }

    pub async fn write_chunk(&mut self, ctx: &Ctx, chunk: Vec<u8>) -> Result<()> {
        match self.driver.as_mut().ok_or(HttpError::BodyClosed)? {
            OutgoingDriver::Server { handle, .. } => handle.write_chunk(ctx, chunk).await,
        }
    }

    pub async fn finish(mut self, ctx: &Ctx) -> Result<()> {
        match self.driver.take() {
            Some(OutgoingDriver::Server { handle, .. }) => {
                let result = handle.finish(ctx).await;
                if result.is_err() {
                    handle.abort();
                }
                result
            }
            None => Ok(()),
        }
    }
}

impl Drop for IncomingBody {
    fn drop(&mut self) {
        if let Some(BodyDriver::Server {
            handle,
            active: true,
        }) = self.driver.as_ref()
        {
            handle.abort_incoming();
        }
    }
}

impl Drop for OutgoingBody {
    fn drop(&mut self) {
        if let Some(OutgoingDriver::Server {
            handle,
            active: true,
        }) = self.driver.as_ref()
        {
            handle.abort();
        }
    }
}
