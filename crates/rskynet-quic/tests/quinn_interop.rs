use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rcgen::{CertifiedKey, generate_simple_self_signed};
use rskynet_bootstrap::ConfigExt as _;
use rskynet_core::{Builder, Config, Ctx, MsgType, Registry, Result};
use rskynet_net::RegistryExt as _;
use rskynet_quic::{
    PrivateKeyInput, QuicEvent, QuicServerOptions, RegistryExt as _, ServerTlsConfig,
};
use rskynet_timer::BuilderExt as _;

struct Echo {
    address: Arc<Mutex<Option<SocketAddr>>>,
    tls: ServerTlsConfig,
}

#[rskynet_macros::service(crate = ::rskynet_core)]
impl Echo {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let listener = rskynet_quic::listen(
            &ctx,
            QuicServerOptions::new("127.0.0.1:0", self.tls.clone()),
        )
        .await?;
        *self.address.lock().unwrap() = Some(rskynet_quic::local_addr(&ctx, listener).await?);
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
                let _ = rskynet_quic::start(&ctx, id).await;
            }
            QuicEvent::StreamData { id, stream, data } => {
                if rskynet_quic::send_wait(&ctx, id, stream, data)
                    .await
                    .is_ok()
                {
                    let task_ctx = ctx.clone();
                    ctx.spawn(async move {
                        task_ctx.sleep_ms(100).await;
                        task_ctx.abort();
                    });
                }
            }
            _ => {}
        }
    }
}

#[test]
fn quinn_client_interoperates_with_rskynet_server() {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(["localhost".to_string()]).unwrap();
    let certificate = rskynet_quic::CertificateDer::from(cert.der().to_vec());
    let server = ServerTlsConfig::single_cert(
        vec![certificate.clone()],
        PrivateKeyInput::PlainPem(signing_key.serialize_pem().into()),
        Vec::new(),
    )
    .unwrap();

    let address = Arc::new(Mutex::new(None));
    let service_address = address.clone();
    let node = std::thread::spawn(move || {
        let registry = Registry::new()
            .with_net()
            .with_quic()
            .with("echo", move || Echo {
                address: service_address.clone(),
                tls: server.clone(),
            });
        let mut config = Config::default().with_bootstrap(["echo"]);
        config
            .section_mut("logger")
            .insert("name".into(), "".into());
        config
            .section_mut("signal")
            .insert("name".into(), "".into());
        Builder::new(config)
            .registry(registry)
            .with_wheel_timer()
            .service("bootstrap", || rskynet_bootstrap::Bootstrap)
            .startup_service(rskynet_net::NAME, "")
            .startup_service(rskynet_quic::NAME, "")
            .run()
            .unwrap();
    });

    let deadline = Instant::now() + Duration::from_secs(5);
    let server_address = loop {
        if let Some(value) = *address.lock().unwrap() {
            break value;
        }
        assert!(Instant::now() < deadline, "QUIC server 未在期限内启动");
        std::thread::sleep(Duration::from_millis(10));
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async move {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate).unwrap();
        let client = quinn::ClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(client);
        let connection = endpoint
            .connect(server_address, "localhost")
            .unwrap()
            .await
            .unwrap();
        let (mut send, mut recv) = connection.open_bi().await.unwrap();
        send.write_all(b"quinn interop").await.unwrap();
        send.finish().unwrap();
        let mut echoed = vec![0; b"quinn interop".len()];
        recv.read_exact(&mut echoed).await.unwrap();
        assert_eq!(echoed, b"quinn interop");
        connection.close(0u32.into(), b"done");
        endpoint.wait_idle().await;
    });

    node.join().unwrap();
}
