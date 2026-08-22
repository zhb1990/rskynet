//! rskynet 的过程宏：把 [`Service`] 实现里的样板消掉。
//!
//! 要消的样板有两处，都是 trait 里还不能写 `async fn` 逼出来的——内核收的是
//! `BoxFuture`，于是每个方法都得裹一层 `Box::pin(async move { .. })`，接收者也得
//! 写成 `self: Arc<Self>`：
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
//! 加上 [`#[service]`][service] 之后是这样：
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
//! # 三件事
//!
//! - [`#[service]`][service]：认 `init` / `dispatch` 两个方法，生成 `impl Service`。
//! - [`#[exclusive]`][exclusive]：再认 `idle` / `interrupt`，同时生成 `impl Service`
//!   与 `impl Exclusive`。后两个是同步方法（它们跑在自己那条线程上），原样搬过去。
//! - `#[msg(..)]`：标在方法上取代手写 `dispatch`，宏按协议号生成分发，见
//!   [`#[service]`][service] 的文档。
//! - `#[debug]`：与 `#[msg(..)]` 一起使用，把强类型消息显式开放给 Dashboard。
//!
//! 没被认领的方法留在原来的 inherent `impl` 块里，一个字都不动。
//!
//! # 生成的代码引哪个 crate
//!
//! 默认是 `::rskynet`，也就是门面 crate——使用方依赖的通常就是它。本仓内部那几个
//! crate 只依赖 `rskynet-core`（依赖门面会成环），要用宏就得写明：
//!
//! ```ignore
//! #[rskynet_macros::service(crate = ::rskynet_core)]
//! impl Echo { .. }
//! ```
//!
//! [`Service`]: https://docs.rs/rskynet-core/latest/rskynet_core/trait.Service.html

mod cluster;
mod expand;
mod signal;

use proc_macro::TokenStream;

/// 为 Prost 消息实现 `cluster::ClusterMessage`。
#[proc_macro_derive(ClusterMessage, attributes(cluster))]
pub fn cluster_message(item: TokenStream) -> TokenStream {
    cluster::derive_message(item.into()).into()
}

/// 把一个无捕获的自由 `async fn` 注册为 cluster handler。
#[proc_macro_attribute]
pub fn cluster_handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    cluster::handler(attr.into(), item.into()).into()
}

/// 从 inherent `impl` 块生成 `impl Service`。
///
/// 认这两个方法名，其余原样留下：
///
/// | 写法 | 生成 |
/// | --- | --- |
/// | `async fn init(&self, ctx: Ctx, args: String) -> Result<()>` | `Service::init` |
/// | `async fn dispatch(&self, ctx: Ctx, msg: Message)` | `Service::dispatch` |
///
/// 尾部参数不关心就别写：`async fn init(&self, ctx: Ctx) -> Result<()>` 一样能用。
/// 两个都不写也行——`dispatch` 会生成一个「没人认领就回错误」的默认实现，于是
/// 只有 `init` 的服务（引导服务那种）不必再写一句空 `Box::pin`。
///
/// # 按协议号分发
///
/// 不想自己写 `dispatch` 的话，把处理函数标上 `#[msg(..)]`：
///
/// ```ignore
/// #[rskynet::service]
/// impl Gate {
///     // 负载按声明的类型取，取不出来就给对方回一个错误应答
///     #[msg(MsgType::SOCKET)]
///     async fn on_socket(&self, ctx: Ctx, event: SocketEvent) { .. }
///
///     // 有返回值且对方在等回话 → 自动 ctx.reply
///     #[msg(MsgType::USER)]
///     async fn on_ask(&self, ctx: Ctx, ask: Ask) -> Answer { .. }
///
///     // 请求从网页 JSON 反序列化，call 返回值再序列化为 JSON。
///     #[debug(name = "ask", example = r#"{"question":"status"}"#)]
///     #[msg(MsgType::USER)]
///     async fn debug_ask(&self, ctx: Ctx, ask: Ask) -> Answer { .. }
///
///     // 一个处理函数可以认多个协议号
///     #[msg(MsgType::TEXT, MsgType::SYSTEM)]
///     async fn on_text(&self, ctx: Ctx, text: String) { .. }
///
///     // 其余协议号的兜底。参数写 Message 就是整条消息都交给它
///     #[msg(default)]
///     async fn on_other(&self, ctx: Ctx, msg: Message) { .. }
/// }
/// ```
///
/// 参数类型决定负载怎么取：写 `Message` 是整条消息（回包也归它自己管），写别的
/// 类型走 [`FromPayload`]；`Vec<u8>` / `String` 对应字节负载，自己的类型写一句
/// `boxed_payload!(Ask);` 就对应对象负载。返回值走 [`IntoPayload`]，返回 `()`
/// 就不回包。
///
/// `#[debug]` 是显式 opt-in：请求类型还需实现 `serde::de::DeserializeOwned`，有返回值
/// 时返回类型还需实现 `serde::Serialize + FromPayload`。返回 `()` 的处理器只支持
/// Dashboard 的 send；收整条 `Message` 与 `#[msg(default)]` 不能开放给调试控制台。
///
/// 没有 `#[msg(default)]` 时，认不出协议号的消息会记一行日志，对方在等回话的话
/// 还会收到一个错误应答——总比让它永久挂着好。
///
/// 写上 `name = "echo"` 会把服务提交到链接期自动注册表，默认通过 `Default`
/// 构造。特殊构造方式可写 `factory = make_echo`；不写 `name` 时完全不自动注册。
///
/// [`FromPayload`]: https://docs.rs/rskynet-core/latest/rskynet_core/trait.FromPayload.html
/// [`IntoPayload`]: https://docs.rs/rskynet-core/latest/rskynet_core/trait.IntoPayload.html
#[proc_macro_attribute]
pub fn service(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand::expand(attr.into(), item.into(), expand::Flavor::Shared).into()
}

/// 同 [`#[service]`][service]，另外生成 `impl Exclusive`——独占一条线程的服务用它。
///
/// 多认两个方法，都是同步的：
///
/// | 写法 | 生成 |
/// | --- | --- |
/// | `fn idle(&self, ctx: &Ctx, idler: &Idler)` | `Exclusive::idle` |
/// | `fn interrupt(&self)` | `Exclusive::interrupt` |
///
/// 两个都可以不写（trait 各有默认实现）：`idle` 默认 `idler.park()`，`interrupt`
/// 默认什么都不做。纯消息驱动的独占服务（日志那种）于是只写 `init` / `dispatch`
/// 就行。
///
/// ```ignore
/// #[rskynet::exclusive]
/// impl Poller {
///     async fn init(&self, ctx: Ctx) -> Result<()> { .. }
///
///     #[msg(MsgType::USER)]
///     async fn on_command(&self, ctx: Ctx, cmd: Command) { .. }
///
///     fn idle(&self, ctx: &Ctx, idler: &Idler) {
///         // 跑在自己那条线程上，可以放心阻塞
///     }
///
///     fn interrupt(&self) {
///         // 会被任意线程调用，只许碰能跨线程的东西
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn exclusive(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand::expand(attr.into(), item.into(), expand::Flavor::Exclusive).into()
}

/// 只是个占位，好让 `#[msg]` 写错地方时报一句人话。
///
/// 真正处理它的是 [`#[service]`][service] 与 [`#[exclusive]`][exclusive]：属性宏
/// 由外往里展开，那两个宏拿到整个 `impl` 块时就把 `#[msg]` 摘走了，所以这个函数
/// 只会在 `#[msg]` 落单时被调到。
#[proc_macro_attribute]
pub fn msg(_attr: TokenStream, _item: TokenStream) -> TokenStream {
    expand::stray_msg().into()
}

/// 把一个 `#[msg(...)]` 处理器显式开放给 Dashboard 调试控制台。
///
/// 该属性由外层 [`service`] / [`exclusive`] 宏读取，单独使用会报错。
#[proc_macro_attribute]
pub fn debug(_attr: TokenStream, _item: TokenStream) -> TokenStream {
    expand::stray_debug().into()
}

/// 注册一个进程信号回调。
///
/// ```ignore
/// #[rskynet::signal(SIGTERM)]
/// fn on_term(ctx: &rskynet::Ctx) {
///     ctx.abort();
/// }
/// ```
///
/// 回调必须是同步的 `fn(&Ctx)`。同一最终二进制中每种信号只能注册一次；宏会为
/// 信号生成唯一导出符号，重复注册会让编译或链接失败。
#[proc_macro_attribute]
pub fn signal(attr: TokenStream, item: TokenStream) -> TokenStream {
    signal::expand(attr.into(), item.into()).into()
}
