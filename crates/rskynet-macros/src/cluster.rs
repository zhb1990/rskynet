use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{
    Error, FnArg, GenericArgument, Ident, ItemFn, LitInt, LitStr, Path, PathArguments, Result,
    ReturnType, Token, Type, TypePath, parse_quote,
};

pub(crate) fn derive_message(item: TokenStream) -> TokenStream {
    match try_derive_message(item) {
        Ok(tokens) => tokens,
        Err(error) => error.to_compile_error(),
    }
}

fn try_derive_message(item: TokenStream) -> Result<TokenStream> {
    let input: syn::DeriveInput = syn::parse2(item)?;
    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &input.generics,
            "泛型消息不能自动实现 ClusterMessage",
        ));
    }
    let mut type_id = None;
    let mut krate: Option<Path> = None;
    for attr in input
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("cluster"))
    {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("type_id") {
                if type_id.is_some() {
                    return Err(meta.error("`type_id` 写了两遍"));
                }
                type_id = Some(meta.value()?.parse::<LitInt>()?);
                Ok(())
            } else if meta.path.is_ident("crate") {
                if krate.is_some() {
                    return Err(meta.error("`crate` 写了两遍"));
                }
                krate = Some(meta.value()?.parse()?);
                Ok(())
            } else {
                Err(meta.error("只认 `type_id` 与 `crate`"))
            }
        })?;
    }
    let type_id = type_id
        .ok_or_else(|| Error::new_spanned(&input.ident, "缺少 `#[cluster(type_id = ...)]`"))?;
    let value = type_id.base10_parse::<u32>()?;
    if value == 0 {
        return Err(Error::new_spanned(
            type_id,
            "ClusterMessage TYPE_ID 不能为 0",
        ));
    }
    let krate = krate.unwrap_or_else(|| parse_quote!(::rskynet::cluster));
    let ident = input.ident;
    Ok(quote! {
        impl #krate::ClusterMessage for #ident {
            const TYPE_ID: u32 = #value;
        }
    })
}

pub(crate) fn handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    match try_handler(attr, item) {
        Ok(tokens) => tokens,
        Err(error) => error.to_compile_error(),
    }
}

enum Target {
    Name(LitStr),
    Handle(LitInt),
}

struct HandlerArgs {
    target: Target,
    krate: Path,
}

impl syn::parse::Parse for HandlerArgs {
    fn parse(input: syn::parse::ParseStream) -> Result<Self> {
        let target = if input.peek(LitStr) {
            Target::Name(input.parse()?)
        } else if input.peek(LitInt) {
            let value: LitInt = input.parse()?;
            value.base10_parse::<u64>()?;
            Target::Handle(value)
        } else {
            return Err(input.error("目标要写字符串服务名或 u32 handle 字面量"));
        };
        let mut krate = None;
        if !input.is_empty() {
            input.parse::<Token![,]>()?;
            input.parse::<Token![crate]>()?;
            input.parse::<Token![=]>()?;
            krate = Some(input.parse()?);
        }
        if !input.is_empty() {
            return Err(input.error("多余的 handler 参数"));
        }
        Ok(Self {
            target,
            krate: krate.unwrap_or_else(|| parse_quote!(::rskynet::cluster)),
        })
    }
}

fn try_handler(attr: TokenStream, item: TokenStream) -> Result<TokenStream> {
    let HandlerArgs { target, krate } = syn::parse2(attr)?;
    let function: ItemFn = syn::parse2(item)?;
    if function.sig.asyncness.is_none() {
        return Err(Error::new_spanned(
            function.sig.fn_token,
            "cluster handler 必须是 async fn",
        ));
    }
    if !function.sig.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &function.sig.generics,
            "cluster handler 不能是泛型函数",
        ));
    }
    if function.sig.inputs.len() != 2 {
        return Err(Error::new_spanned(
            &function.sig.inputs,
            "cluster handler 必须接收 (RemoteContext, Request) 两个参数",
        ));
    }
    let mut inputs = function.sig.inputs.iter();
    let Some(FnArg::Typed(_remote)) = inputs.next() else {
        return Err(Error::new(
            Span::call_site(),
            "cluster handler 不能有 self 参数",
        ));
    };
    let Some(FnArg::Typed(request)) = inputs.next() else {
        return Err(Error::new(
            Span::call_site(),
            "cluster handler 缺少请求参数",
        ));
    };
    let request_ty = &request.ty;
    let response_ty = result_ok_type(&function.sig.output)?;
    let name = &function.sig.ident;
    let register = Ident::new(&format!("__rskynet_cluster_register_{name}"), name.span());
    let (target_value, descriptor) = match target {
        Target::Name(target_name) => (
            quote!(#target_name),
            quote!(#krate::AutoHandler::name(
                #target_name,
                ::core::concat!(::core::file!(), ":", ::core::line!()),
                #register,
            )),
        ),
        Target::Handle(handle) => {
            let value = handle.base10_parse::<u64>()?;
            (
                quote!(#value),
                quote!(#krate::AutoHandler::handle(
                    #value,
                    ::core::concat!(::core::file!(), ":", ::core::line!()),
                    #register,
                )),
            )
        }
    };
    let registration = if is_unit(&response_ty) {
        quote! {
            registry.register_send::<#request_ty, _, _>(#target_value, |remote, request| #name(remote, request))?;
        }
    } else {
        quote! {
            registry.register::<#request_ty, #response_ty, _, _>(#target_value, |remote, request| #name(remote, request))?;
        }
    };
    Ok(quote! {
        #function

        const _: () = {
            fn #register(registry: &mut #krate::HandlerRegistry) -> ::core::result::Result<(), #krate::ClusterError> {
                #registration
                ::core::result::Result::Ok(())
            }

            #krate::__private::inventory::submit! { #descriptor }
        };
    })
}

fn result_ok_type(output: &ReturnType) -> Result<Type> {
    let ReturnType::Type(_, ty) = output else {
        return Err(Error::new(
            Span::call_site(),
            "cluster handler 必须返回 Result<Response, String>",
        ));
    };
    let Type::Path(TypePath { path, .. }) = ty.as_ref() else {
        return Err(Error::new_spanned(
            ty,
            "cluster handler 必须返回 Result<Response, String>",
        ));
    };
    let Some(segment) = path.segments.last() else {
        return Err(Error::new_spanned(path, "cluster handler 返回类型不合法"));
    };
    if segment.ident != "Result" {
        return Err(Error::new_spanned(
            path,
            "cluster handler 必须返回 Result<Response, String>",
        ));
    }
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return Err(Error::new_spanned(segment, "Result 必须写明类型参数"));
    };
    if args.args.len() != 2 {
        return Err(Error::new_spanned(
            args,
            "Result 必须是 Result<Response, String>",
        ));
    }
    let Some(GenericArgument::Type(ok)) = args.args.first() else {
        return Err(Error::new_spanned(args, "Result 的第一个参数必须是类型"));
    };
    let Some(GenericArgument::Type(Type::Path(error))) = args.args.last() else {
        return Err(Error::new_spanned(args, "Result 的错误类型必须是 String"));
    };
    if error
        .path
        .segments
        .last()
        .is_none_or(|segment| segment.ident != "String")
    {
        return Err(Error::new_spanned(error, "Result 的错误类型必须是 String"));
    }
    Ok(ok.clone())
}

fn is_unit(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(tuple) if tuple.elems.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    #[test]
    fn message_requires_a_nonzero_type_id() {
        assert!(
            try_derive_message(quote!(
                struct Missing;
            ))
            .is_err()
        );
        assert!(
            try_derive_message(quote!(
                #[cluster(type_id = 0)]
                struct Zero;
            ))
            .is_err()
        );
    }

    #[test]
    fn handler_signature_is_checked() {
        assert!(
            try_handler(
                quote!("x"),
                quote!(
                    fn sync() {}
                )
            )
            .is_err()
        );
        assert!(
            try_handler(
                quote!("x"),
                quote!(
                    async fn missing(request: Request) -> Result<Response, String> {
                        todo!()
                    }
                )
            )
            .is_err()
        );
        assert!(
            try_handler(
                quote!("x"),
                quote!(
                    async fn wrong(a: RemoteContext, b: Request) -> Response {
                        todo!()
                    }
                )
            )
            .is_err()
        );
    }
}
