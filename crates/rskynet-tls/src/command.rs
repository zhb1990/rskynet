use rustls::pki_types::ServerName;

use rskynet_core::{Ctx, Error, FromPayload, MsgType, Payload, Result, boxed_payload};

use crate::NAME;
use crate::event::{TlsId, TlsInfo};
use crate::identity::{ClientTlsConfig, ServerTlsConfig};

#[derive(Debug)]
pub struct ClientOptions {
    pub address: String,
    pub server_name: ServerName<'static>,
    pub config: ClientTlsConfig,
    pub connect_timeout_ms: Option<u64>,
    pub handshake_timeout_ms: Option<u64>,
}

impl ClientOptions {
    pub fn new(
        address: impl Into<String>,
        server_name: ServerName<'static>,
        config: ClientTlsConfig,
    ) -> Self {
        Self {
            address: address.into(),
            server_name,
            config,
            connect_timeout_ms: None,
            handshake_timeout_ms: None,
        }
    }

    /// 限制底层 TCP 建连等待时间。
    #[must_use]
    pub fn with_connect_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.connect_timeout_ms = Some(timeout_ms);
        self
    }

    /// 覆盖本次客户端连接的 TLS 握手超时。
    #[must_use]
    pub fn with_handshake_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.handshake_timeout_ms = Some(timeout_ms);
        self
    }
}

#[derive(Debug)]
pub struct ServerOptions {
    pub address: String,
    pub config: ServerTlsConfig,
}

impl ServerOptions {
    pub fn new(address: impl Into<String>, config: ServerTlsConfig) -> Self {
        Self {
            address: address.into(),
            config,
        }
    }
}

#[derive(Debug)]
pub enum Command {
    Connect(ClientOptions),
    Listen(ServerOptions),
    Start(TlsId),
    Pause(TlsId),
    Send {
        id: TlsId,
        data: Vec<u8>,
        high: bool,
    },
    SendWait {
        id: TlsId,
        data: Vec<u8>,
        high: bool,
    },
    Close(TlsId),
    Shutdown(TlsId),
    Info(TlsId),
    #[doc(hidden)]
    HandshakeTimeout(TlsId),
    #[doc(hidden)]
    CloseFailed {
        id: TlsId,
        reason: String,
    },
}

boxed_payload!(Command);

#[derive(Debug)]
pub enum Answer {
    Id(TlsId),
    Info(TlsInfo),
    Done,
    Failed(String),
}

boxed_payload!(Answer);

pub async fn connect(ctx: &Ctx, options: ClientOptions) -> Result<TlsId> {
    ask_id(ctx, Command::Connect(options)).await
}

pub async fn listen(ctx: &Ctx, options: ServerOptions) -> Result<TlsId> {
    ask_id(ctx, Command::Listen(options)).await
}

pub async fn start(ctx: &Ctx, id: TlsId) -> Result<()> {
    ask_done(ctx, Command::Start(id)).await
}

/// 暂停从 TLS 连接读取明文；用 [`start`] 恢复。
pub async fn pause(ctx: &Ctx, id: TlsId) -> Result<()> {
    ask_done(ctx, Command::Pause(id)).await
}

pub fn send(ctx: &Ctx, id: TlsId, data: Vec<u8>) -> Result<()> {
    tell(
        ctx,
        Command::Send {
            id,
            data,
            high: true,
        },
    )
}

pub fn send_low(ctx: &Ctx, id: TlsId, data: Vec<u8>) -> Result<()> {
    tell(
        ctx,
        Command::Send {
            id,
            data,
            high: false,
        },
    )
}

/// 发送明文，并在底层 TCP 写队列拥塞时等待它回落。
pub async fn send_wait(ctx: &Ctx, id: TlsId, data: Vec<u8>) -> Result<()> {
    ask_done(
        ctx,
        Command::SendWait {
            id,
            data,
            high: true,
        },
    )
    .await
}

pub async fn send_low_wait(ctx: &Ctx, id: TlsId, data: Vec<u8>) -> Result<()> {
    ask_done(
        ctx,
        Command::SendWait {
            id,
            data,
            high: false,
        },
    )
    .await
}

pub async fn close(ctx: &Ctx, id: TlsId) -> Result<()> {
    ask_done(ctx, Command::Close(id)).await
}

pub fn shutdown(ctx: &Ctx, id: TlsId) -> Result<()> {
    tell(ctx, Command::Shutdown(id))
}

pub async fn info(ctx: &Ctx, id: TlsId) -> Result<TlsInfo> {
    match ask(ctx, Command::Info(id)).await? {
        Answer::Info(info) => Ok(info),
        answer => Err(unexpected(answer)),
    }
}

async fn ask(ctx: &Ctx, command: Command) -> Result<Answer> {
    let reply = ctx.call(NAME, MsgType::USER, Payload::of(command)).await?;
    match Answer::from_payload(reply)? {
        Answer::Failed(reason) => Err(Error::Service(reason)),
        answer => Ok(answer),
    }
}

fn tell(ctx: &Ctx, command: Command) -> Result<()> {
    ctx.send(NAME, MsgType::USER, Payload::of(command))
}

async fn ask_id(ctx: &Ctx, command: Command) -> Result<TlsId> {
    match ask(ctx, command).await? {
        Answer::Id(id) => Ok(id),
        answer => Err(unexpected(answer)),
    }
}

async fn ask_done(ctx: &Ctx, command: Command) -> Result<()> {
    match ask(ctx, command).await? {
        Answer::Done => Ok(()),
        answer => Err(unexpected(answer)),
    }
}

fn unexpected(answer: Answer) -> Error {
    Error::Service(format!("TLS 服务回了不匹配的应答：{answer:?}"))
}
