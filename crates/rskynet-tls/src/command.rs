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
        }
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
    Send {
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
