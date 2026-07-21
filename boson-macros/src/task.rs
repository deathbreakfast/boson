//! Implementation of the `#[boson::task]` proc macro.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;

pub fn task_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    match task_impl_impl(attr.into(), item.into()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

fn task_impl_impl(attr: TokenStream2, item: TokenStream2) -> syn::Result<TokenStream2> {
    let attrs: crate::task_attrs::TaskAttrs = syn::parse2(attr)?;
    let input: syn::ItemFn = syn::parse2(item)?;
    crate::task_validate::validate_signature(&input.sig)?;
    Ok(crate::task_expand::expand_task(&attrs, &input))
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    #[test]
    fn test_to_pascal_case() {
        assert_eq!(crate::task_expand::to_pascal_case("test_echo"), "TestEcho");
        assert_eq!(
            crate::task_expand::to_pascal_case("notify_user"),
            "NotifyUser"
        );
        assert_eq!(crate::task_expand::to_pascal_case("simple"), "Simple");
    }

    #[test]
    fn test_deserialization_is_generated_for_both_param_shapes() {
        let unit_tokens = task_impl_impl(
            quote!(name = "noop"),
            quote! {
                pub async fn noop(ctx: Box<dyn boson_core::ExecutionContext>) -> boson_core::Result<()> {
                    let _ = ctx;
                    Ok(())
                }
            },
        )
        .expect("unit-params macro expansion should succeed")
        .to_string();

        let with_params_tokens = task_impl_impl(
            quote!(name = "with_params"),
            quote! {
                pub async fn with_params(
                    ctx: Box<dyn boson_core::ExecutionContext>,
                    user_id: String,
                ) -> boson_core::Result<()> {
                    let _ = (ctx, user_id);
                    Ok(())
                }
            },
        )
        .expect("non-unit params macro expansion should succeed")
        .to_string();

        assert!(unit_tokens.contains("serde_json :: from_value"));
        assert!(with_params_tokens.contains("serde_json :: from_value"));
    }

    #[test]
    fn test_compile_pass_contract() {
        let tokens = task_impl_impl(
            quote!(name = "notify_user"),
            quote! {
                pub async fn notify_user(
                    ctx: Box<dyn boson_core::ExecutionContext>,
                    user_id: String,
                ) -> boson_core::Result<()> {
                    let _ = (ctx, user_id);
                    Ok(())
                }
            },
        )
        .expect("valid task signature should pass");

        let expanded = tokens.to_string();
        assert!(expanded.contains("struct NotifyUserParams"));
        assert!(expanded.contains("struct NotifyUser"));
        assert!(expanded.contains("actor_json : serde_json :: Value"));
    }

    #[test]
    fn test_compile_fail_missing_name_attribute() {
        let error = task_impl_impl(
            quote!(),
            quote! {
                pub async fn missing_name(
                    ctx: Box<dyn boson_core::ExecutionContext>,
                ) -> boson_core::Result<()> {
                    let _ = ctx;
                    Ok(())
                }
            },
        )
        .expect_err("missing name attribute must fail");

        let message = error.to_string();
        assert!(
            message.contains("expected `name`")
                || message.contains("unexpected end of input")
                || message.contains("unexpected token"),
            "unexpected parse error: {message}"
        );
    }

    #[test]
    fn test_compile_fail_non_async_function() {
        let error = task_impl_impl(
            quote!(name = "not_async"),
            quote! {
                pub fn not_async(ctx: Box<dyn boson_core::ExecutionContext>) -> boson_core::Result<()> {
                    let _ = ctx;
                    Ok(())
                }
            },
        )
        .expect_err("non-async task must fail");

        assert!(error.to_string().contains("must be async"));
    }

    #[test]
    fn test_compile_fail_first_param_not_execution_context() {
        let error = task_impl_impl(
            quote!(name = "wrong_first_param"),
            quote! {
                pub async fn wrong_first_param(user_id: String) -> boson_core::Result<()> {
                    let _ = user_id;
                    Ok(())
                }
            },
        )
        .expect_err("first parameter mismatch must fail");

        assert!(error.to_string().contains("Box<dyn ExecutionContext>"));
    }

    #[test]
    fn test_compile_fail_return_type_not_result_unit() {
        let error = task_impl_impl(
            quote!(name = "wrong_return_type"),
            quote! {
                pub async fn wrong_return_type(
                    ctx: Box<dyn boson_core::ExecutionContext>,
                ) -> boson_core::Result<String> {
                    let _ = ctx;
                    Ok("bad".to_string())
                }
            },
        )
        .expect_err("wrong return type must fail");

        assert!(error.to_string().contains("return type must be Result<()>"));
    }
}
