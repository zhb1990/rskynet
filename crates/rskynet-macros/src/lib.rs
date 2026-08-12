//! rskynet 的过程宏。
//!
//! **本 crate 目前是空的**：crate 划分先落地，宏本体另开一轮。
//!
//! 要消掉的样板是 `Service` 实现里的这两处——trait 里的 `async fn` 还不能做成
//! trait object，所以内核收的是 `BoxFuture`，写起来就得裹一层 `Box::pin`，
//! 接收者也得写成 `self: Arc<Self>`：
//!
//! ```ignore
//! impl Service for Echo {
//!     fn dispatch(self: Arc<Self>, ctx: Ctx, mut msg: Message) -> BoxFuture<'static, ()> {
//!         Box::pin(async move {
//!             let _ = ctx.reply(&msg, msg.take_payload());
//!         })
//!     }
//! }
//! ```
//!
//! 打算做成：
//!
//! ```ignore
//! #[rskynet::service]
//! impl Echo {
//!     async fn dispatch(&self, ctx: Ctx, mut msg: Message) {
//!         let _ = ctx.reply(&msg, msg.take_payload());
//!     }
//! }
//! ```
//!
//! 生成的代码引用 `::rskynet::` 这个绝对路径，也就是门面 crate；本仓内部的
//! crate（它们只依赖 `rskynet-core`，依赖门面会成环）要用时得靠
//! `#[service(crate = ::rskynet_core)]` 覆盖。
