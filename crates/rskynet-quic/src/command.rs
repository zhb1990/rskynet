use std::net::SocketAddr;
use std::sync::Arc;

use quinn_proto::crypto::rustls::{
    QuicClientConfig as ProtoClientCrypto, QuicServerConfig as ProtoServerCrypto,
};
use quinn_proto::{ClientConfig as ProtoClientConfig, ServerConfig as ProtoServerConfig, VarInt};
use rskynet_core::{Ctx, Error, FromPayload, MsgType, Payload, Result, boxed_payload};
use rskynet_tls::{ClientTlsConfig, ServerTlsConfig};

use crate::NAME;
use crate::config::QuicTransportOptions;
use crate::event::{QuicConnectionId, QuicInfo, QuicListenerId, QuicStreamId};

pub struct QuicClientOptions {
    pub address: String,
    pub bind: Option<String>,
    pub server_name: String,
    pub tls: ClientTlsConfig,
    pub transport: QuicTransportOptions,
    pub connect_timeout_ms: Option<u64>,
}

impl QuicClientOptions {
    pub fn new(
        address: impl Into<String>,
        server_name: impl Into<String>,
        tls: ClientTlsConfig,
    ) -> Self {
        Self {
            address: address.into(),
            bind: None,
            server_name: server_name.into(),
            tls,
            transport: QuicTransportOptions::default(),
            connect_timeout_ms: None,
        }
    }

    #[must_use]
    pub fn with_bind(mut self, bind: impl Into<String>) -> Self {
        self.bind = Some(bind.into());
        self
    }

    #[must_use]
    pub fn with_transport(mut self, transport: QuicTransportOptions) -> Self {
        self.transport = transport;
        self
    }

    #[must_use]
    pub fn with_connect_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.connect_timeout_ms = Some(timeout_ms);
        self
    }

    pub(crate) fn proto_config(&self) -> Result<ProtoClientConfig> {
        let rustls = self.tls.rustls_config();
        let crypto = ProtoClientCrypto::try_from(rustls.as_ref().clone())
            .map_err(|error| Error::Service(format!("QUIC 客户端 TLS 配置无效：{error}")))?;
        let mut config = ProtoClientConfig::new(Arc::new(crypto));
        config.transport_config(self.transport.build()?);
        Ok(config)
    }
}

pub struct QuicServerOptions {
    pub address: String,
    pub tls: ServerTlsConfig,
    pub transport: QuicTransportOptions,
}

impl QuicServerOptions {
    pub fn new(address: impl Into<String>, tls: ServerTlsConfig) -> Self {
        Self {
            address: address.into(),
            tls,
            transport: QuicTransportOptions::default(),
        }
    }

    #[must_use]
    pub fn with_transport(mut self, transport: QuicTransportOptions) -> Self {
        self.transport = transport;
        self
    }

    pub(crate) fn proto_config(&self) -> Result<ProtoServerConfig> {
        let rustls = self.tls.rustls_config();
        let crypto = ProtoServerCrypto::try_from(rustls.as_ref().clone())
            .map_err(|error| Error::Service(format!("QUIC 服务端 TLS 配置无效：{error}")))?;
        let mut config = ProtoServerConfig::with_crypto(Arc::new(crypto));
        config.transport = self.transport.build()?;
        Ok(config)
    }
}

pub enum Command {
    Listen(QuicServerOptions),
    ListenerLocalAddr(QuicListenerId),
    CloseListener(QuicListenerId),
    Connect(QuicClientOptions),
    Start(QuicConnectionId),
    OpenBi(QuicConnectionId),
    OpenUni(QuicConnectionId),
    Send {
        id: QuicConnectionId,
        stream: QuicStreamId,
        data: Vec<u8>,
        wait: bool,
    },
    Finish {
        id: QuicConnectionId,
        stream: QuicStreamId,
    },
    Reset {
        id: QuicConnectionId,
        stream: QuicStreamId,
        error_code: u64,
    },
    Stop {
        id: QuicConnectionId,
        stream: QuicStreamId,
        error_code: u64,
    },
    SendDatagram {
        id: QuicConnectionId,
        data: Vec<u8>,
    },
    Close {
        id: QuicConnectionId,
        error_code: u64,
        reason: Vec<u8>,
    },
    Shutdown(QuicConnectionId),
    Info(QuicConnectionId),
    Netstat,
    #[doc(hidden)]
    Tick {
        socket: rskynet_net::SocketId,
        generation: u64,
    },
    #[doc(hidden)]
    ConnectTimeout(QuicConnectionId),
}

boxed_payload!(Command);

#[derive(Debug)]
pub enum Answer {
    Listener(QuicListenerId),
    LocalAddr(SocketAddr),
    Connection(QuicConnectionId),
    Stream(QuicStreamId),
    Info(QuicInfo),
    Infos(Vec<QuicInfo>),
    Done,
    Failed(String),
}

boxed_payload!(Answer);

pub async fn listen(ctx: &Ctx, options: QuicServerOptions) -> Result<QuicListenerId> {
    match ask(ctx, Command::Listen(options)).await? {
        Answer::Listener(id) => Ok(id),
        other => Err(unexpected(other)),
    }
}

pub async fn connect(ctx: &Ctx, options: QuicClientOptions) -> Result<QuicConnectionId> {
    match ask(ctx, Command::Connect(options)).await? {
        Answer::Connection(id) => Ok(id),
        other => Err(unexpected(other)),
    }
}

/// 查询 listener 实际绑定的地址；绑定端口 0 时用它取得系统分配的端口。
pub async fn local_addr(ctx: &Ctx, id: QuicListenerId) -> Result<SocketAddr> {
    match ask(ctx, Command::ListenerLocalAddr(id)).await? {
        Answer::LocalAddr(address) => Ok(address),
        other => Err(unexpected(other)),
    }
}

/// 停止接受新连接。已建立的连接保持可用，它们全部排空后底层 UDP 端点关闭。
pub async fn close_listener(ctx: &Ctx, id: QuicListenerId) -> Result<()> {
    ask_done(ctx, Command::CloseListener(id)).await
}

pub async fn start(ctx: &Ctx, id: QuicConnectionId) -> Result<()> {
    ask_done(ctx, Command::Start(id)).await
}

pub async fn open_bi(ctx: &Ctx, id: QuicConnectionId) -> Result<QuicStreamId> {
    ask_stream(ctx, Command::OpenBi(id)).await
}

pub async fn open_uni(ctx: &Ctx, id: QuicConnectionId) -> Result<QuicStreamId> {
    ask_stream(ctx, Command::OpenUni(id)).await
}

pub fn send(ctx: &Ctx, id: QuicConnectionId, stream: QuicStreamId, data: Vec<u8>) -> Result<()> {
    ctx.send(
        NAME,
        MsgType::USER,
        Payload::of(Command::Send {
            id,
            stream,
            data,
            wait: false,
        }),
    )
}

pub async fn send_wait(
    ctx: &Ctx,
    id: QuicConnectionId,
    stream: QuicStreamId,
    data: Vec<u8>,
) -> Result<()> {
    ask_done(
        ctx,
        Command::Send {
            id,
            stream,
            data,
            wait: true,
        },
    )
    .await
}

pub async fn finish(ctx: &Ctx, id: QuicConnectionId, stream: QuicStreamId) -> Result<()> {
    ask_done(ctx, Command::Finish { id, stream }).await
}

pub async fn reset(
    ctx: &Ctx,
    id: QuicConnectionId,
    stream: QuicStreamId,
    error_code: u64,
) -> Result<()> {
    VarInt::from_u64(error_code).map_err(|_| Error::Service("QUIC error code 超出范围".into()))?;
    ask_done(
        ctx,
        Command::Reset {
            id,
            stream,
            error_code,
        },
    )
    .await
}

pub async fn stop(
    ctx: &Ctx,
    id: QuicConnectionId,
    stream: QuicStreamId,
    error_code: u64,
) -> Result<()> {
    VarInt::from_u64(error_code).map_err(|_| Error::Service("QUIC error code 超出范围".into()))?;
    ask_done(
        ctx,
        Command::Stop {
            id,
            stream,
            error_code,
        },
    )
    .await
}

pub async fn send_datagram(ctx: &Ctx, id: QuicConnectionId, data: Vec<u8>) -> Result<()> {
    ask_done(ctx, Command::SendDatagram { id, data }).await
}

pub async fn close(
    ctx: &Ctx,
    id: QuicConnectionId,
    error_code: u64,
    reason: Vec<u8>,
) -> Result<()> {
    VarInt::from_u64(error_code).map_err(|_| Error::Service("QUIC error code 超出范围".into()))?;
    ask_done(
        ctx,
        Command::Close {
            id,
            error_code,
            reason,
        },
    )
    .await
}

pub fn shutdown(ctx: &Ctx, id: QuicConnectionId) -> Result<()> {
    ctx.send(NAME, MsgType::USER, Payload::of(Command::Shutdown(id)))
}

pub async fn info(ctx: &Ctx, id: QuicConnectionId) -> Result<QuicInfo> {
    match ask(ctx, Command::Info(id)).await? {
        Answer::Info(info) => Ok(info),
        other => Err(unexpected(other)),
    }
}

pub async fn netstat(ctx: &Ctx) -> Result<Vec<QuicInfo>> {
    match ask(ctx, Command::Netstat).await? {
        Answer::Infos(infos) => Ok(infos),
        other => Err(unexpected(other)),
    }
}

async fn ask(ctx: &Ctx, command: Command) -> Result<Answer> {
    let reply = ctx.call(NAME, MsgType::USER, Payload::of(command)).await?;
    match Answer::from_payload(reply)? {
        Answer::Failed(reason) => Err(Error::Service(reason)),
        answer => Ok(answer),
    }
}

async fn ask_done(ctx: &Ctx, command: Command) -> Result<()> {
    match ask(ctx, command).await? {
        Answer::Done => Ok(()),
        other => Err(unexpected(other)),
    }
}

async fn ask_stream(ctx: &Ctx, command: Command) -> Result<QuicStreamId> {
    match ask(ctx, command).await? {
        Answer::Stream(id) => Ok(id),
        other => Err(unexpected(other)),
    }
}

fn unexpected(answer: Answer) -> Error {
    Error::Service(format!("QUIC 服务回了不匹配的应答：{answer:?}"))
}
