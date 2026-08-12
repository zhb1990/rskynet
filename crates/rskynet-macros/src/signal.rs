use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Error, FnArg, Ident, ItemFn, ReturnType, Type, parse2};

const SUPPORTED: &[&str] = &[
    "SIGINT", "SIGTERM", "SIGHUP", "SIGQUIT", "SIGUSR1", "SIGUSR2",
];

const FATAL: &[&str] = &["SIGSEGV", "SIGABRT", "SIGBUS", "SIGILL", "SIGFPE"];

pub(crate) fn expand(attr: TokenStream, item: TokenStream) -> TokenStream {
    match try_expand(attr, item) {
        Ok(tokens) => tokens,
        Err(err) => err.to_compile_error(),
    }
}

fn try_expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let signal: Ident = parse2(attr)?;
    let signal_name = signal.to_string();
    if FATAL.contains(&signal_name.as_str()) {
        return Err(Error::new_spanned(
            signal,
            "致命信号由 rskynet 崩溃处理器独占，不能注册业务回调",
        ));
    }
    if !SUPPORTED.contains(&signal_name.as_str()) {
        return Err(Error::new_spanned(
            signal,
            "只支持 SIGINT、SIGTERM、SIGHUP、SIGQUIT、SIGUSR1、SIGUSR2",
        ));
    }

    let function: ItemFn = parse2(item)?;
    validate(&function)?;
    let name = &function.sig.ident;
    let variant = match signal_name.as_str() {
        "SIGINT" => format_ident!("Interrupt"),
        "SIGTERM" => format_ident!("Terminate"),
        "SIGHUP" => format_ident!("Hangup"),
        "SIGQUIT" => format_ident!("Quit"),
        "SIGUSR1" => format_ident!("User1"),
        "SIGUSR2" => format_ident!("User2"),
        _ => unreachable!(),
    };
    let wrapper = format_ident!(
        "__rskynet_signal_callback_{}_{}",
        signal_name.to_ascii_lowercase(),
        name
    );
    let symbol = format!("__rskynet_signal_{signal_name}");

    Ok(quote! {
        #function

        #[doc(hidden)]
        fn #wrapper(ctx: &::rskynet::Ctx) {
            #name(ctx)
        }

        const _: () = {
            #[used]
            #[unsafe(export_name = #symbol)]
            static UNIQUE_SIGNAL_REGISTRATION: u8 = 0;

            ::rskynet::__private::inventory::submit! {
                ::rskynet::signal::AutoSignal::new(
                    ::rskynet::signal::Signal::#variant,
                    concat!(module_path!(), "::", stringify!(#name)),
                    #wrapper,
                )
            }
        };
    })
}

fn validate(function: &ItemFn) -> syn::Result<()> {
    let sig = &function.sig;
    if sig.asyncness.is_some() {
        return Err(Error::new_spanned(sig.asyncness, "信号回调不能是 async fn"));
    }
    if sig.unsafety.is_some() {
        return Err(Error::new_spanned(sig.unsafety, "信号回调不能是 unsafe fn"));
    }
    if !sig.generics.params.is_empty() || sig.generics.where_clause.is_some() {
        return Err(Error::new_spanned(&sig.generics, "信号回调不能带泛型"));
    }
    if !matches!(sig.output, ReturnType::Default) {
        return Err(Error::new_spanned(&sig.output, "信号回调必须返回 ()"));
    }
    if sig.inputs.len() != 1 {
        return Err(Error::new_spanned(
            &sig.inputs,
            "信号回调必须只接收一个 &Ctx",
        ));
    }
    let Some(FnArg::Typed(arg)) = sig.inputs.first() else {
        return Err(Error::new_spanned(
            &sig.inputs,
            "信号回调必须是自由函数 fn(&Ctx)",
        ));
    };
    let Type::Reference(reference) = arg.ty.as_ref() else {
        return Err(Error::new_spanned(&arg.ty, "信号回调参数必须是 &Ctx"));
    };
    if reference.mutability.is_some() {
        return Err(Error::new_spanned(
            &arg.ty,
            "信号回调参数必须是共享引用 &Ctx",
        ));
    }
    let Type::Path(path) = reference.elem.as_ref() else {
        return Err(Error::new_spanned(&arg.ty, "信号回调参数必须是 &Ctx"));
    };
    if path
        .path
        .segments
        .last()
        .is_none_or(|segment| segment.ident != "Ctx")
    {
        return Err(Error::new_spanned(&arg.ty, "信号回调参数必须是 &Ctx"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    #[test]
    fn valid_callback_exports_a_signal_unique_symbol() {
        let output = try_expand(
            quote!(SIGTERM),
            quote!(
                fn stop(_ctx: &rskynet::Ctx) {}
            ),
        )
        .expect("合法回调应展开成功")
        .to_string();
        assert!(output.contains("__rskynet_signal_SIGTERM"));
        assert!(output.contains("AutoSignal"));
    }

    #[test]
    fn fatal_and_invalid_signatures_are_rejected() {
        assert!(
            try_expand(
                quote!(SIGSEGV),
                quote!(
                    fn crash(_ctx: &rskynet::Ctx) {}
                )
            )
            .is_err()
        );
        assert!(
            try_expand(
                quote!(SIGTERM),
                quote!(
                    async fn stop(_ctx: &rskynet::Ctx) {}
                )
            )
            .is_err()
        );
        assert!(
            try_expand(
                quote!(SIGTERM),
                quote!(
                    fn stop() -> bool {
                        true
                    }
                )
            )
            .is_err()
        );
    }
}
