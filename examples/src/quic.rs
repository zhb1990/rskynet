use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use rcgen::{CertifiedKey, generate_simple_self_signed};
use rskynet::quic::{
    CertificateDer, ClientTlsConfig, PrivateKeyInput, QuicClientOptions, QuicEvent,
    QuicServerOptions, ServerTlsConfig, ServerVerification,
};
use rskynet::{Ctx, MsgType, Result, SvcCell};

#[derive(Default)]
struct QuicExample {
    received: Arc<Mutex<Vec<u8>>>,
    inbound: SvcCell<HashSet<rskynet::quic::QuicConnectionId>>,
}

#[rskynet::service(name = "quic-example")]
impl QuicExample {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let CertifiedKey { cert, signing_key } =
            generate_simple_self_signed(["localhost".to_string()])
                .map_err(|error| rskynet::Error::Service(error.to_string()))?;
        let certificate = CertificateDer::from(cert.der().to_vec());
        let server = ServerTlsConfig::single_cert(
            vec![certificate.clone()],
            PrivateKeyInput::PlainPem(signing_key.serialize_pem().into()),
            vec![b"rskynet-echo".to_vec()],
        )
        .map_err(|error| rskynet::Error::Service(error.to_string()))?;
        let client = ClientTlsConfig::new(
            ServerVerification::CustomRoots {
                roots: vec![certificate],
            },
            vec![b"rskynet-echo".to_vec()],
        )
        .map_err(|error| rskynet::Error::Service(error.to_string()))?;
        let listener =
            rskynet::quic::listen(&ctx, QuicServerOptions::new("127.0.0.1:0", server)).await?;
        let address = rskynet::quic::local_addr(&ctx, listener).await?;
        rskynet::log!(ctx, "QUIC 回声服务监听 {address}");

        let received = self.received.clone();
        let task_ctx = ctx.clone();
        ctx.spawn(async move {
            let result: Result<()> = async {
                let connection = rskynet::quic::connect(
                    &task_ctx,
                    QuicClientOptions::new(address.to_string(), "localhost", client),
                )
                .await?;
                let stream = rskynet::quic::open_bi(&task_ctx, connection).await?;
                rskynet::quic::send_wait(&task_ctx, connection, stream, b"hello quic".to_vec())
                    .await?;
                rskynet::quic::finish(&task_ctx, connection, stream).await?;
                for _ in 0..500 {
                    if received.lock().unwrap().as_slice() == b"hello quic" {
                        rskynet::log!(task_ctx, "QUIC 回声成功");
                        return Ok(());
                    }
                    task_ctx.sleep_ms(10).await;
                }
                Err(rskynet::Error::Service("QUIC 回声超时".into()))
            }
            .await;
            if let Err(error) = result {
                rskynet::log!(task_ctx, "QUIC 示例失败：{error}");
            }
            task_ctx.abort();
        });
        Ok(())
    }

    #[msg(MsgType::QUIC)]
    async fn on_quic(&self, ctx: Ctx, event: QuicEvent) {
        match event {
            QuicEvent::Connected {
                id,
                listener: Some(_),
                ..
            } => {
                self.inbound.borrow_mut().insert(id);
                let _ = rskynet::quic::start(&ctx, id).await;
            }
            QuicEvent::StreamData { id, stream, data } => {
                if self.inbound.borrow().contains(&id) {
                    let _ = rskynet::quic::send_wait(&ctx, id, stream, data).await;
                } else {
                    self.received.lock().unwrap().extend(data);
                }
            }
            _ => {}
        }
    }
}
