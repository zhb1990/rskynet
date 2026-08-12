use rskynet::net::{self, SocketEvent};
use rskynet::{Ctx, MsgType, Result};

#[derive(Default)]
struct Echo;

#[rskynet::service(name = "echo")]
impl Echo {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let listener = net::listen(&ctx, "127.0.0.1:8888").await?;
        net::start(&ctx, listener).await?;
        rskynet::log!(ctx, "回声服务器监听 127.0.0.1:8888");
        Ok(())
    }

    #[msg(MsgType::SOCKET)]
    async fn on_socket(&self, ctx: Ctx, event: SocketEvent) {
        match event {
            SocketEvent::Accept { id, peer, .. } => {
                rskynet::log!(ctx, "接受 {peer}：{id}");
                let _ = net::start(&ctx, id).await;
                let _ = net::set_nodelay(&ctx, id, true);
            }
            SocketEvent::Data { id, data } => {
                let _ = net::send(&ctx, id, data);
            }
            SocketEvent::Close { id } => rskynet::log!(ctx, "{id} 已关闭"),
            SocketEvent::Error { id, reason } => rskynet::log!(ctx, "{id} 出错：{reason}"),
            SocketEvent::Warning { id, kilobytes } => {
                rskynet::log!(ctx, "{id} 写缓冲已堆到 {kilobytes} KiB")
            }
            SocketEvent::Udp { .. } => {}
        }
    }
}
