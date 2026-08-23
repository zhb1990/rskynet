//! `#[service_plugin]`：为 service 内插件生成命令分发、静态注册与 Dashboard 描述。

use std::collections::{HashMap, HashSet};

use proc_macro2::{Span, TokenStream};
use quote::{ToTokens, format_ident, quote};
use syn::{
    Error, Expr, FnArg, Ident, ImplItem, ImplItemFn, ItemImpl, LitStr, Meta, PatType, Path, Result,
    ReturnType, Token, Type, parse_quote,
};

pub(crate) fn expand(attr: TokenStream, item: TokenStream) -> TokenStream {
    match try_expand(attr, item) {
        Ok(tokens) => tokens,
        Err(error) => error.to_compile_error(),
    }
}

struct Args {
    krate: Path,
    namespace: LitStr,
    name: LitStr,
    factory: Path,
    dependencies: Vec<LitStr>,
    debug_all: bool,
}

impl syn::parse::Parse for Args {
    fn parse(input: syn::parse::ParseStream<'_>) -> Result<Self> {
        let mut krate = None;
        let mut namespace = None;
        let mut name = None;
        let mut factory = None;
        let mut dependencies = None;
        let mut debug_all = false;
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            match key.to_string().as_str() {
                "debug" => {
                    if debug_all {
                        return Err(Error::new_spanned(key, "`debug` 写了两遍"));
                    }
                    debug_all = true;
                }
                "crate" | "namespace" | "name" | "factory" | "dependencies" => {
                    input.parse::<Token![=]>()?;
                    match key.to_string().as_str() {
                        "crate" => krate = Some(input.parse()?),
                        "namespace" => namespace = Some(input.parse()?),
                        "name" => name = Some(input.parse()?),
                        "factory" => factory = Some(input.parse()?),
                        "dependencies" => {
                            let content;
                            syn::bracketed!(content in input);
                            let values = content
                                .parse_terminated(|input| input.parse(), Token![,])?
                                .into_iter()
                                .collect();
                            dependencies = Some(values);
                        }
                        _ => unreachable!(),
                    }
                }
                _ => return Err(Error::new_spanned(key, "不认识的 service_plugin 参数")),
            }
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(Self {
            krate: krate.unwrap_or_else(|| parse_quote!(::rskynet)),
            namespace: namespace
                .ok_or_else(|| Error::new(Span::call_site(), "缺少 `namespace = \"...\"`"))?,
            name: name.ok_or_else(|| Error::new(Span::call_site(), "缺少 `name = \"...\"`"))?,
            factory: factory
                .ok_or_else(|| Error::new(Span::call_site(), "缺少 `factory = Type::new`"))?,
            dependencies: dependencies.unwrap_or_default(),
            debug_all,
        })
    }
}

struct MsgArgs {
    command: Expr,
    variant: Option<Path>,
}

impl syn::parse::Parse for MsgArgs {
    fn parse(input: syn::parse::ParseStream<'_>) -> Result<Self> {
        let command = input.parse()?;
        let mut variant = None;
        if !input.is_empty() {
            input.parse::<Token![,]>()?;
            let key: Ident = input.parse()?;
            if key != "variant" {
                return Err(Error::new_spanned(
                    key,
                    "这里只支持 `variant = Enum::Variant`",
                ));
            }
            input.parse::<Token![=]>()?;
            variant = Some(input.parse()?);
        }
        if !input.is_empty() {
            return Err(input.error("一个插件处理器只能声明一个命令"));
        }
        Ok(Self { command, variant })
    }
}

struct DebugAttr(Option<LitStr>);

struct Handler {
    method: Ident,
    command: Expr,
    variant: Option<(Path, Path)>,
    takes_ctx: bool,
    arg: Option<Type>,
    response: Option<Type>,
    debug_name: Option<LitStr>,
}

fn try_expand(attr: TokenStream, item: TokenStream) -> Result<TokenStream> {
    let args: Args = syn::parse2(attr)?;
    let mut block: ItemImpl = syn::parse2(item)?;
    if block.trait_.is_some() {
        return Err(Error::new_spanned(
            &block.impl_token,
            "service_plugin 要标在 inherent impl 上",
        ));
    }
    let mut handlers = Vec::new();
    let mut methods = Vec::new();
    let mut has_mount = false;
    let mut has_init = false;

    for item in std::mem::take(&mut block.items) {
        let ImplItem::Fn(mut method) = item else {
            methods.push(item);
            continue;
        };
        let route = take_msg(&mut method)?;
        let debug = take_debug(&mut method)?;
        if let Some(route) = route {
            handlers.push(parse_handler(&method, route, debug, args.debug_all)?);
        } else if debug.is_some() {
            return Err(Error::new_spanned(
                &method.sig.ident,
                "`#[debug]` 只能与 `#[msg(...)]` 一起使用",
            ));
        } else {
            has_mount |= method.sig.ident == "mount";
            has_init |= method.sig.ident == "init";
        }
        methods.push(ImplItem::Fn(method));
    }
    block.items = methods;

    let krate = &args.krate;
    let self_ty = &block.self_ty;
    let (impl_generics, _, where_clause) = block.generics.split_for_impl();
    let dispatch = dispatch(krate, &handlers)?;
    let debug_messages = debug_messages(krate, &args.name, &handlers);
    let mount = has_mount.then(|| {
        quote! {
            fn mount(
                self: ::std::sync::Arc<Self>,
                mount: &mut #krate::plugin::PluginMount<'_>,
            ) -> #krate::Result<()> {
                Self::mount(&self, mount)
            }
        }
    });
    let init = has_init.then(|| {
        quote! {
            fn init(
                self: ::std::sync::Arc<Self>,
                ctx: #krate::plugin::PluginCtx,
                config: #krate::plugin::__private::TomlValue,
            ) -> #krate::BoxFuture<'static, #krate::Result<()>> {
                ::std::boxed::Box::pin(async move { Self::init(&self, ctx, config).await })
            }
        }
    });

    let mut seen = HashSet::new();
    let commands: Vec<_> = handlers
        .iter()
        .filter(|handler| seen.insert(handler.command.to_token_stream().to_string()))
        .map(|handler| &handler.command)
        .collect();
    let namespace = &args.namespace;
    let name = &args.name;
    let factory = &args.factory;
    let dependencies = &args.dependencies;

    Ok(quote! {
        #block

        impl #impl_generics #krate::plugin::ServicePlugin for #self_ty #where_clause {
            #mount
            #init
            #dispatch
            #debug_messages
        }

        #krate::plugin::register_service_plugin! {
            namespace: #namespace,
            name: #name,
            plugin: #self_ty,
            factory: #factory,
            dependencies: [#(#dependencies),*],
            commands: [#(#commands),*],
        }
    })
}

fn take_msg(method: &mut ImplItemFn) -> Result<Option<MsgArgs>> {
    let indexes: Vec<_> = method
        .attrs
        .iter()
        .enumerate()
        .filter_map(|(index, attr)| attr.path().is_ident("msg").then_some(index))
        .collect();
    if indexes.len() > 1 {
        return Err(Error::new_spanned(
            &method.attrs[indexes[1]],
            "`#[msg]` 写了两遍",
        ));
    }
    let Some(index) = indexes.first().copied() else {
        return Ok(None);
    };
    let attr = method.attrs.remove(index);
    attr.parse_args().map(Some)
}

fn take_debug(method: &mut ImplItemFn) -> Result<Option<DebugAttr>> {
    let indexes: Vec<_> = method
        .attrs
        .iter()
        .enumerate()
        .filter_map(|(index, attr)| attr.path().is_ident("debug").then_some(index))
        .collect();
    if indexes.len() > 1 {
        return Err(Error::new_spanned(
            &method.attrs[indexes[1]],
            "`#[debug]` 写了两遍",
        ));
    }
    let Some(index) = indexes.first().copied() else {
        return Ok(None);
    };
    let attr = method.attrs.remove(index);
    let mut name = None;
    match &attr.meta {
        Meta::Path(_) => {}
        Meta::List(_) => attr.parse_nested_meta(|meta| {
            if !meta.path.is_ident("name") {
                return Err(meta.error("只支持 `name = \"...\"`"));
            }
            name = Some(meta.value()?.parse()?);
            Ok(())
        })?,
        Meta::NameValue(_) => return Err(Error::new_spanned(attr, "写成 `#[debug]`")),
    }
    Ok(Some(DebugAttr(name)))
}

fn parse_handler(
    method: &ImplItemFn,
    route: MsgArgs,
    debug: Option<DebugAttr>,
    debug_all: bool,
) -> Result<Handler> {
    if method.sig.asyncness.is_none() {
        return Err(Error::new_spanned(
            &method.sig.ident,
            "插件 `#[msg]` 处理器必须是 async fn",
        ));
    }
    let typed: Vec<&PatType> = method
        .sig
        .inputs
        .iter()
        .filter_map(|arg| match arg {
            FnArg::Typed(arg) => Some(arg),
            _ => None,
        })
        .collect();
    if typed.len() > 2 {
        return Err(Error::new_spanned(
            &method.sig.ident,
            "参数最多是 PluginCtx 与负载",
        ));
    }
    let takes_ctx = !typed.is_empty();
    let arg = typed.get(1).map(|arg| (*arg.ty).clone());
    let response = match &method.sig.output {
        ReturnType::Default => None,
        ReturnType::Type(_, ty) if matches!(&**ty, Type::Tuple(tuple) if tuple.elems.is_empty()) => {
            None
        }
        ReturnType::Type(_, ty) => Some((**ty).clone()),
    };
    let variant = route
        .variant
        .map(|path| {
            if path.segments.len() < 2 {
                return Err(Error::new_spanned(path, "variant 要写完整路径"));
            }
            let mut envelope = path.clone();
            envelope.segments.pop();
            envelope.segments.pop_punct();
            Ok((path, envelope))
        })
        .transpose()?;
    let debug_name =
        if let Some(DebugAttr(name)) = debug {
            Some(name.unwrap_or_else(|| {
                LitStr::new(&method.sig.ident.to_string(), method.sig.ident.span())
            }))
        } else if debug_all {
            Some(LitStr::new(
                &method.sig.ident.to_string(),
                method.sig.ident.span(),
            ))
        } else {
            None
        };
    Ok(Handler {
        method: method.sig.ident.clone(),
        command: route.command,
        variant,
        takes_ctx,
        arg,
        response,
        debug_name,
    })
}

fn finish(krate: &Path, handler: &Handler, call: TokenStream) -> TokenStream {
    if handler.response.is_some() {
        quote! {
            let __out = #call.await;
            if #krate::Message::needs_reply(&msg) {
                #krate::Ctx::reply(
                    ctx.service(), &msg, #krate::IntoPayload::into_payload(__out),
                )?;
            }
        }
    } else {
        quote!(#call.await;)
    }
}

fn call(krate: &Path, handler: &Handler, value: Option<TokenStream>) -> TokenStream {
    let method = &handler.method;
    let ctx = handler
        .takes_ctx
        .then(|| quote!(::core::clone::Clone::clone(&ctx),));
    let value = value.unwrap_or_default();
    finish(krate, handler, quote!(self.#method(#ctx #value)))
}

fn variant_call(krate: &Path, handler: &Handler, value: Option<TokenStream>) -> TokenStream {
    let method = &handler.method;
    let ctx = handler
        .takes_ctx
        .then(|| quote!(::core::clone::Clone::clone(&ctx),));
    let value = value.unwrap_or_default();
    let call = quote!(self.#method(#ctx #value));
    if handler.response.is_some() {
        finish(krate, handler, call)
    } else {
        let label = method.to_string();
        quote! {
            if #krate::Message::needs_reply(&msg) {
                #krate::Ctx::log(
                    ctx.service(),
                    ::std::format!("{} 只接受 send，拒绝 call", #label),
                );
                #krate::Ctx::reply_error(ctx.service(), &msg)?;
            } else {
                #call.await;
            }
        }
    }
}

fn dispatch(krate: &Path, handlers: &[Handler]) -> Result<TokenStream> {
    struct VariantGroup {
        envelope: String,
        variants: HashSet<String>,
    }

    let mut plain = HashSet::new();
    let mut variants: HashMap<String, VariantGroup> = HashMap::new();
    for handler in handlers {
        let command = handler.command.to_token_stream().to_string();
        match &handler.variant {
            None => {
                if variants.contains_key(&command) {
                    return Err(Error::new_spanned(
                        &handler.method,
                        "同一个插件命令不能混用普通 handler 与 variant handler",
                    ));
                }
                if !plain.insert(command) {
                    return Err(Error::new_spanned(
                        &handler.method,
                        "同一个插件命令的普通 handler 写了两遍",
                    ));
                }
            }
            Some((path, envelope)) => {
                if plain.contains(&command) {
                    return Err(Error::new_spanned(
                        &handler.method,
                        "同一个插件命令不能混用普通 handler 与 variant handler",
                    ));
                }
                let envelope = envelope.to_token_stream().to_string();
                let group = variants.entry(command).or_insert_with(|| VariantGroup {
                    envelope: envelope.clone(),
                    variants: HashSet::new(),
                });
                if group.envelope != envelope {
                    return Err(Error::new_spanned(
                        &handler.method,
                        "同一个插件命令的 variant handler 必须属于同一个外层枚举",
                    ));
                }
                if !group.variants.insert(path.to_token_stream().to_string()) {
                    return Err(Error::new_spanned(
                        &handler.method,
                        "同一个 enum variant 写了两遍",
                    ));
                }
            }
        }
    }

    let mut plain = Vec::new();
    let mut groups: HashMap<String, (&Expr, &Path, Vec<&Handler>)> = HashMap::new();
    for handler in handlers {
        if let Some((_, envelope)) = &handler.variant {
            groups
                .entry(handler.command.to_token_stream().to_string())
                .or_insert((&handler.command, envelope, Vec::new()))
                .2
                .push(handler);
        } else {
            let command = &handler.command;
            let body = match &handler.arg {
                None => call(krate, handler, None),
                Some(_) => {
                    let invoke = call(krate, handler, Some(quote!(__arg)));
                    quote! {
                        let __arg = #krate::FromPayload::from_payload(
                            #krate::Message::take_payload(&mut msg),
                        )?;
                        #invoke
                    }
                }
            };
            plain.push(
                quote! { if command == #command { #body return ::core::result::Result::Ok(()); } },
            );
        }
    }
    let variants = groups.values().map(|(command, envelope, handlers)| {
        let arms = handlers.iter().map(|handler| {
            let (path, _) = handler.variant.as_ref().unwrap();
            let (pattern, value) = if handler.arg.is_some() {
                (quote!(#path(__value)), Some(quote!(__value)))
            } else {
                (quote!(#path), None)
            };
            let body = variant_call(krate, handler, value);
            quote!(#pattern => { #body })
        });
        quote! {
            if command == #command {
                let __message: #envelope = #krate::FromPayload::from_payload(
                    #krate::Message::take_payload(&mut msg),
                )?;
                match __message { #(#arms,)* _ => {
                    return ::core::result::Result::Err(#krate::Error::service(
                        "插件命令的 enum variant 没有处理器",
                    ));
                }}
                return ::core::result::Result::Ok(());
            }
        }
    });
    Ok(quote! {
        fn handle(
            self: ::std::sync::Arc<Self>,
            ctx: #krate::plugin::PluginCtx,
            command: #krate::plugin::CommandId,
            mut msg: #krate::Message,
        ) -> #krate::BoxFuture<'static, #krate::Result<()>> {
            ::std::boxed::Box::pin(async move {
                #(#plain)*
                #(#variants)*
                ::core::result::Result::Err(#krate::Error::service(::std::format!(
                    "插件收到未知命令 {}", command.0,
                )))
            })
        }
    })
}

fn debug_messages(krate: &Path, plugin_name: &LitStr, handlers: &[Handler]) -> TokenStream {
    let exposed: Vec<_> = handlers
        .iter()
        .filter(|handler| handler.debug_name.is_some())
        .collect();
    let decoders = exposed.iter().enumerate().map(|(index, handler)| {
        let decoder = format_ident!("__rskynet_plugin_decode_{index}");
        let command = &handler.command;
        let build = match (&handler.arg, &handler.variant) {
            (None, None) => quote! { #krate::__private::decode_json::<()>(value)?; () },
            (Some(ty), None) => quote!(#krate::__private::decode_json::<#ty>(value)?),
            (None, Some((path, _))) => {
                quote! { #krate::__private::decode_json::<()>(value)?; #path }
            }
            (Some(ty), Some((path, _))) => {
                quote!(#path(#krate::__private::decode_json::<#ty>(value)?))
            }
        };
        quote! {
            fn #decoder(value: #krate::__private::JsonValue) -> #krate::Result<#krate::Payload> {
                let payload = #krate::IntoPayload::into_payload({ #build });
                ::core::result::Result::Ok(#krate::Payload::of(
                    #krate::plugin::CommandEnvelope::new(#command, payload),
                ))
            }
        }
    });
    let descriptors = exposed.iter().enumerate().map(|(index, handler)| {
        let decoder = format_ident!("__rskynet_plugin_decode_{index}");
        let label = handler.debug_name.as_ref().unwrap();
        let name = quote!(::core::concat!(#plugin_name, ".", #label));
        let request: Type = handler.arg.clone().unwrap_or_else(|| parse_quote!(()));
        match &handler.response {
            Some(response) => {
                quote!(#krate::DebugMessageDescriptor::call_decoded::<#request, #response>(
                    #name, #krate::MsgType::USER, #decoder,
                ))
            }
            None => quote!(#krate::DebugMessageDescriptor::send_decoded::<#request>(
                #name, #krate::MsgType::USER, #decoder,
            )),
        }
    });
    quote! {
        fn debug_messages(&self) -> ::std::vec::Vec<#krate::DebugMessageDescriptor> {
            #(#decoders)*
            ::std::vec![#(#descriptors),*]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    fn args() -> TokenStream {
        quote! {
            namespace = "test",
            name = "demo",
            factory = Demo::default,
            dependencies = [],
        }
    }

    #[test]
    fn duplicate_plain_commands_are_rejected() {
        let error = try_expand(
            args(),
            quote! {
                impl Demo {
                    #[msg(COMMAND)]
                    async fn first(&self, ctx: PluginCtx) {}
                    #[msg(COMMAND)]
                    async fn second(&self, ctx: PluginCtx) {}
                }
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("普通 handler 写了两遍"));
    }

    #[test]
    fn variant_commands_reject_ambiguous_groups() {
        let mixed = try_expand(
            args(),
            quote! {
                impl Demo {
                    #[msg(COMMAND)]
                    async fn plain(&self, ctx: PluginCtx) {}
                    #[msg(COMMAND, variant = Request::Ping)]
                    async fn variant(&self, ctx: PluginCtx) {}
                }
            },
        )
        .unwrap_err();
        assert!(mixed.to_string().contains("不能混用"));

        let different_envelope = try_expand(
            args(),
            quote! {
                impl Demo {
                    #[msg(COMMAND, variant = First::Ping)]
                    async fn first(&self, ctx: PluginCtx) {}
                    #[msg(COMMAND, variant = Second::Pong)]
                    async fn second(&self, ctx: PluginCtx) {}
                }
            },
        )
        .unwrap_err();
        assert!(different_envelope.to_string().contains("同一个外层枚举"));

        let duplicate = try_expand(
            args(),
            quote! {
                impl Demo {
                    #[msg(COMMAND, variant = Request::Ping)]
                    async fn first(&self, ctx: PluginCtx) {}
                    #[msg(COMMAND, variant = Request::Ping)]
                    async fn second(&self, ctx: PluginCtx) {}
                }
            },
        )
        .unwrap_err();
        assert!(duplicate.to_string().contains("variant 写了两遍"));
    }
}
