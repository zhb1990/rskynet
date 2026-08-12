//! `#[service]` / `#[exclusive]` 的展开。
//!
//! 输入是一个 inherent `impl` 块，输出最多三块：留着原样的 `impl`（放没被认领的
//! 方法）、`impl Service`、`impl Exclusive`。认领规则只看方法名与 `#[msg]` 标注，
//! 见 [`Hook`] 与 [`Route`]。

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::{
    Error, Expr, FnArg, Ident, ImplItem, ImplItemFn, ItemImpl, PatType, Path, Result, ReturnType,
    Signature, Token, Type, parse_quote,
};

/// 注册方式：共享 worker 池还是独占一条线程。决定要不要认 `idle` / `interrupt`。
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Flavor {
    Shared,
    Exclusive,
}

/// 被宏认领的钩子，按方法名认。
#[derive(Clone, Copy, PartialEq, Eq)]
enum Hook {
    Init,
    Dispatch,
    Idle,
    Interrupt,
}

/// `#[msg(..)]` 里写的东西。
enum Route {
    /// 一个或多个协议号，展开成 `if mtype == .. || mtype == ..`。
    Types(Vec<Expr>),
    /// `#[msg(default)]`，其余协议号的兜底。
    Fallback,
}

pub(crate) fn expand(attr: TokenStream, item: TokenStream, flavor: Flavor) -> TokenStream {
    match try_expand(attr, item, flavor) {
        Ok(tokens) => tokens,
        Err(err) => err.to_compile_error(),
    }
}

/// 宏参数，目前只有一个 `crate = <路径>`。
struct Args {
    krate: Path,
}

impl syn::parse::Parse for Args {
    fn parse(input: syn::parse::ParseStream) -> Result<Self> {
        let mut krate = None;
        while !input.is_empty() {
            if input.peek(Token![crate]) {
                input.parse::<Token![crate]>()?;
                input.parse::<Token![=]>()?;
                krate = Some(input.parse()?);
            } else {
                return Err(input.error("只认 `crate = <路径>` 这一个参数"));
            }
            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
        }
        // 默认引门面 crate：使用方绝大多数时候依赖的是它
        Ok(Args {
            krate: krate.unwrap_or_else(|| parse_quote!(::rskynet)),
        })
    }
}

fn try_expand(attr: TokenStream, item: TokenStream, flavor: Flavor) -> Result<TokenStream> {
    let Args { krate } = syn::parse2(attr)?;
    let mut block: ItemImpl = syn::parse2(item)?;

    if let Some((_, path, _)) = &block.trait_ {
        return Err(Error::new_spanned(
            path,
            "标在 inherent impl 块上，不是 trait 实现——`impl Service for X` 本来就没有样板可消",
        ));
    }

    let mut hooks: Vec<(Hook, ImplItemFn)> = Vec::new();
    let mut handlers: Vec<Handler> = Vec::new();
    let mut leftovers: Vec<ImplItem> = Vec::new();

    for item in std::mem::take(&mut block.items) {
        let ImplItem::Fn(mut func) = item else {
            leftovers.push(item);
            continue;
        };
        let route = take_route(&mut func)?;
        let hook = hook_of(&func.sig.ident, flavor)?;
        match (hook, route) {
            (Some(_), Some(_)) => {
                return Err(Error::new_spanned(
                    &func.sig.ident,
                    "钩子方法上不能再标 `#[msg]`：它本来就是内核直接调的",
                ));
            }
            (Some(hook), None) => hooks.push((hook, func)),
            (None, Some(route)) => handlers.push(Handler::parse(func, route, &mut leftovers)?),
            (None, None) => leftovers.push(ImplItem::Fn(func)),
        }
    }

    let has_dispatch = hooks.iter().any(|(hook, _)| *hook == Hook::Dispatch);
    if has_dispatch && !handlers.is_empty() {
        return Err(Error::new(
            Span::call_site(),
            "`dispatch` 与 `#[msg]` 只能选一种：要么自己分发，要么让宏按协议号分发",
        ));
    }

    let mut init = None;
    let mut dispatch = None;
    let mut idle = None;
    let mut interrupt = None;
    for (hook, func) in hooks {
        let generated = match hook {
            Hook::Init => &mut init,
            Hook::Dispatch => &mut dispatch,
            Hook::Idle => &mut idle,
            Hook::Interrupt => &mut interrupt,
        };
        if generated.is_some() {
            return Err(Error::new_spanned(&func.sig.ident, "同一个钩子写了两遍"));
        }
        *generated = Some(match hook {
            Hook::Init => async_hook(
                &krate,
                func,
                &[
                    parse_quote!(#krate::Ctx),
                    parse_quote!(::std::string::String),
                ],
                parse_quote!(#krate::Result<()>),
            )?,
            Hook::Dispatch => async_hook(
                &krate,
                func,
                &[parse_quote!(#krate::Ctx), parse_quote!(#krate::Message)],
                parse_quote!(()),
            )?,
            Hook::Idle => sync_hook(
                func,
                &[parse_quote!(&#krate::Ctx), parse_quote!(&#krate::Idler)],
            )?,
            Hook::Interrupt => sync_hook(func, &[])?,
        });
    }

    // 没写 dispatch 也没标 #[msg] 时照样生成一个：内核那边 dispatch 没有默认实现，
    // 而「只有 init 的服务」是常见形态（引导服务就是），不该逼着写一句空 Box::pin
    if dispatch.is_none() {
        dispatch = Some(routed_dispatch(&krate, &handlers)?);
    }

    // `self_ty` 自带类型参数，所以 `ty_generics` 用不上
    let (impl_generics, _, where_clause) = block.generics.split_for_impl();
    let self_ty = &block.self_ty;
    let attrs = &block.attrs;

    let inherent = (!leftovers.is_empty()).then(|| {
        quote! {
            #(#attrs)*
            impl #impl_generics #self_ty #where_clause {
                #(#leftovers)*
            }
        }
    });

    let exclusive = (flavor == Flavor::Exclusive).then(|| {
        quote! {
            impl #impl_generics #krate::Exclusive for #self_ty #where_clause {
                #idle
                #interrupt
            }
        }
    });

    Ok(quote! {
        #inherent

        impl #impl_generics #krate::Service for #self_ty #where_clause {
            #init
            #dispatch
        }

        #exclusive
    })
}

fn hook_of(name: &Ident, flavor: Flavor) -> Result<Option<Hook>> {
    let hook = match name.to_string().as_str() {
        "init" => Hook::Init,
        "dispatch" => Hook::Dispatch,
        "idle" => Hook::Idle,
        "interrupt" => Hook::Interrupt,
        _ => return Ok(None),
    };
    if flavor == Flavor::Shared && matches!(hook, Hook::Idle | Hook::Interrupt) {
        return Err(Error::new_spanned(
            name,
            "`idle` / `interrupt` 只有 `#[exclusive]` 认：共享服务跑在 worker 池上，没有自己那条线程可睡",
        ));
    }
    Ok(Some(hook))
}

/// 摘掉方法上的 `#[msg(..)]` 并解析它。
fn take_route(func: &mut ImplItemFn) -> Result<Option<Route>> {
    let Some(index) = func
        .attrs
        .iter()
        .position(|attr| attr.path().is_ident("msg"))
    else {
        return Ok(None);
    };
    let attr = func.attrs.remove(index);
    let exprs = attr.parse_args_with(Punctuated::<Expr, Comma>::parse_terminated)?;
    if exprs.is_empty() {
        return Err(Error::new_spanned(
            &attr,
            "要写协议号，例如 `#[msg(MsgType::SOCKET)]`，或者写 `#[msg(default)]` 当兜底",
        ));
    }
    let is_default =
        exprs.len() == 1 && matches!(&exprs[0], Expr::Path(path) if path.path.is_ident("default"));
    Ok(Some(if is_default {
        Route::Fallback
    } else {
        Route::Types(exprs.into_iter().collect())
    }))
}

/// 一个 `#[msg]` 处理函数：分发时怎么调它、负载怎么取、返回值要不要回包。
struct Handler {
    name: Ident,
    route: Route,
    /// 第一个参数是 `Ctx`。没有参数的处理函数就不传。
    takes_ctx: bool,
    /// 第二个参数是什么。
    arg: Option<Arg>,
    /// 有返回值，且返回的不是 `()`。
    replies: bool,
    span: Span,
}

/// 处理函数的第二个参数。
enum Arg {
    /// 声明成 `Message`：整条消息交给它，回包也由它自己负责。
    Whole,
    /// 别的类型：走 `FromPayload` 从负载里取。
    Typed(Box<Type>),
}

impl Handler {
    fn parse(func: ImplItemFn, route: Route, leftovers: &mut Vec<ImplItem>) -> Result<Self> {
        let span = func.sig.ident.span();
        if func.sig.asyncness.is_none() {
            return Err(Error::new(span, "`#[msg]` 的处理函数要写成 `async fn`"));
        }
        let args = typed_args(&func.sig)?;
        if args.len() > 2 {
            return Err(Error::new(
                span,
                "`#[msg]` 的处理函数最多两个参数：`ctx: Ctx` 与负载",
            ));
        }
        let arg = args.get(1).map(|arg| {
            if is_message(&arg.ty) {
                Arg::Whole
            } else {
                Arg::Typed(arg.ty.clone())
            }
        });
        let takes_ctx = !args.is_empty();
        let replies = !returns_unit(&func.sig.output);
        if replies && matches!(arg, Some(Arg::Whole)) {
            return Err(Error::new(
                span,
                "收整条 `Message` 的处理函数不能有返回值：消息已经交给它了，自动回包无从下手，请自己调 `ctx.reply`",
            ));
        }

        let name = func.sig.ident.clone();
        // 处理函数本身留在原地，好让它照旧能被直接调用（写单元测试用得上）
        leftovers.push(ImplItem::Fn(func));
        Ok(Handler {
            name,
            route,
            takes_ctx,
            arg,
            replies,
            span,
        })
    }

    /// 命中之后那一段：取负载、调处理函数、按需回包。
    fn body(&self, krate: &Path) -> TokenStream {
        let name = &self.name;
        let ctx_arg = self
            .takes_ctx
            .then(|| quote!(::core::clone::Clone::clone(&ctx),));
        match &self.arg {
            None => self.finish(krate, quote!(self.#name(#ctx_arg))),
            Some(Arg::Whole) => quote!(self.#name(#ctx_arg msg).await;),
            Some(Arg::Typed(ty)) => {
                let call = self.finish(krate, quote!(self.#name(#ctx_arg __arg)));
                let name = name.to_string();
                quote! {
                    match <#ty as #krate::FromPayload>::from_payload(
                        #krate::Message::take_payload(&mut msg),
                    ) {
                        ::core::result::Result::Ok(__arg) => { #call }
                        ::core::result::Result::Err(__err) => {
                            #krate::Ctx::log(&ctx, ::std::format!(
                                "{} 收到不认识的负载：{}", #name, __err,
                            ));
                            if #krate::Message::needs_reply(&msg) {
                                let _ = #krate::Ctx::reply_error(&ctx, &msg);
                            }
                        }
                    }
                }
            }
        }
    }

    fn finish(&self, krate: &Path, call: TokenStream) -> TokenStream {
        if self.replies {
            quote! {
                let __out = #call.await;
                if #krate::Message::needs_reply(&msg) {
                    let _ = #krate::Ctx::reply(
                        &ctx, &msg, #krate::IntoPayload::into_payload(__out),
                    );
                }
            }
        } else {
            quote!(#call.await;)
        }
    }
}

/// 按协议号生成 `dispatch`。
fn routed_dispatch(krate: &Path, handlers: &[Handler]) -> Result<TokenStream> {
    let mut arms = Vec::new();
    let mut fallback = None;
    for handler in handlers {
        let body = handler.body(krate);
        match &handler.route {
            Route::Types(exprs) => {
                let tests = exprs.iter().map(|expr| quote!(__mtype == (#expr)));
                arms.push(quote! {
                    if #(#tests)||* {
                        #body
                        return;
                    }
                });
            }
            Route::Fallback if fallback.is_some() => {
                return Err(Error::new(handler.span, "`#[msg(default)]` 只能有一个"));
            }
            Route::Fallback => fallback = Some(body),
        }
    }

    // 没人认领的消息：发送方要是在等回话，得让它拿到错误而不是永久挂着
    let fallback = fallback.unwrap_or_else(|| {
        quote! {
            #krate::Ctx::log(&ctx, ::std::format!(
                "没人处理 {:?} 消息（来自 :{:08x}）", msg.mtype, msg.source,
            ));
            if #krate::Message::needs_reply(&msg) {
                let _ = #krate::Ctx::reply_error(&ctx, &msg);
            }
        }
    });

    Ok(quote! {
        #[allow(unused_mut, unused_variables)]
        fn dispatch(
            self: ::std::sync::Arc<Self>,
            ctx: #krate::Ctx,
            mut msg: #krate::Message,
        ) -> #krate::BoxFuture<'static, ()> {
            ::std::boxed::Box::pin(async move {
                let __mtype = msg.mtype;
                #(#arms)*
                #fallback
            })
        }
    })
}

/// `async fn(&self, ..)` 改写成 `fn(self: Arc<Self>, ..) -> BoxFuture<'static, R>`。
fn async_hook(
    krate: &Path,
    func: ImplItemFn,
    pads: &[Type],
    default_output: Type,
) -> Result<TokenStream> {
    let span = func.sig.ident.span();
    if func.sig.asyncness.is_none() {
        return Err(Error::new(
            span,
            "钩子要写成 `async fn`——宏正是为了把它裹成 `Box::pin(async move { .. })`",
        ));
    }
    let inputs = rebuild_inputs(&func.sig, pads, parse_quote!(self: ::std::sync::Arc<Self>))?;
    let output = match &func.sig.output {
        ReturnType::Default => default_output,
        ReturnType::Type(_, ty) => (**ty).clone(),
    };
    let attrs = &func.attrs;
    let name = &func.sig.ident;
    let block = &func.block;
    Ok(quote! {
        #(#attrs)*
        fn #name(#inputs) -> #krate::BoxFuture<'static, #output> {
            ::std::boxed::Box::pin(async move #block)
        }
    })
}

/// 同步钩子（`idle` / `interrupt`）原样搬，只补齐没写的参数、去掉可见性。
fn sync_hook(func: ImplItemFn, pads: &[Type]) -> Result<TokenStream> {
    if let Some(asyncness) = func.sig.asyncness {
        return Err(Error::new_spanned(
            asyncness,
            "`idle` / `interrupt` 是同步钩子：它们跑在自己那条线程上，本来就可以直接阻塞",
        ));
    }
    let inputs = rebuild_inputs(&func.sig, pads, parse_quote!(&self))?;
    let attrs = &func.attrs;
    let name = &func.sig.ident;
    let output = &func.sig.output;
    let block = &func.block;
    Ok(quote! {
        #(#attrs)*
        fn #name(#inputs) #output #block
    })
}

/// 换掉接收者，并把没写的尾部参数补成 `_: T`。
///
/// 补齐是为了让「不关心某个参数」不必写出来：`async fn init(&self, ctx: Ctx)` 与
/// `async fn init(&self, ctx: Ctx, _args: String)` 生成的东西一样。
fn rebuild_inputs(
    sig: &Signature,
    pads: &[Type],
    receiver: FnArg,
) -> Result<Punctuated<FnArg, Comma>> {
    let args = typed_args(sig)?;
    if args.len() > pads.len() {
        return Err(Error::new_spanned(
            &sig.inputs,
            format!("`{}` 最多 {} 个参数（除 self）", sig.ident, pads.len()),
        ));
    }
    let mut inputs = Punctuated::new();
    inputs.push(receiver);
    for arg in &args {
        inputs.push(FnArg::Typed((*arg).clone()));
    }
    for ty in &pads[args.len()..] {
        inputs.push(parse_quote!(_: #ty));
    }
    Ok(inputs)
}

/// 校验接收者是 `&self`，并取出其余参数。
fn typed_args(sig: &Signature) -> Result<Vec<&PatType>> {
    let mut args = Vec::new();
    let mut seen_receiver = false;
    for input in &sig.inputs {
        match input {
            FnArg::Receiver(receiver) => {
                if receiver.reference.is_none() || receiver.mutability.is_some() {
                    return Err(Error::new_spanned(
                        receiver,
                        "接收者写 `&self`：宏会按钩子的需要改写成 `self: Arc<Self>`",
                    ));
                }
                seen_receiver = true;
            }
            FnArg::Typed(typed) => args.push(typed),
        }
    }
    if !seen_receiver {
        return Err(Error::new_spanned(
            &sig.inputs,
            "第一个参数得是 `&self`：服务的方法都是实例方法",
        ));
    }
    Ok(args)
}

/// 参数类型是不是 `Message`。只看最后一段，好让 `rskynet::Message` 也算。
fn is_message(ty: &Type) -> bool {
    let Type::Path(path) = ty else { return false };
    path.path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "Message" && segment.arguments.is_none())
}

fn returns_unit(output: &ReturnType) -> bool {
    match output {
        ReturnType::Default => true,
        ReturnType::Type(_, ty) => matches!(&**ty, Type::Tuple(tuple) if tuple.elems.is_empty()),
    }
}

/// 让 `#[msg]` 单独出现时报一句人话，而不是「找不到这个属性」。
pub(crate) fn stray_msg() -> TokenStream {
    Error::new(
        Span::call_site(),
        "`#[msg]` 只在 `#[service]` / `#[exclusive]` 标注的 impl 块里认",
    )
    .to_compile_error()
}
