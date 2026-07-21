use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{FnArg, ItemFn, LitStr, Pat, PatType};

use crate::task_attrs::TaskAttrs;

pub fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            chars.next().map_or_else(String::new, |first| {
                first.to_uppercase().chain(chars).collect()
            })
        })
        .collect()
}

pub fn expand_task(attrs: &TaskAttrs, input: &ItemFn) -> TokenStream2 {
    let task_name = &attrs.name;
    let task_name_lit = LitStr::new(task_name, proc_macro2::Span::call_site());
    let fn_name = &input.sig.ident;
    let fn_vis = &input.vis;
    let fn_block = &input.block;
    let fn_attrs = &input.attrs;
    let fn_sig = &input.sig;

    let priority = attrs.priority;
    let pool_lit = LitStr::new(&attrs.pool, proc_macro2::Span::call_site());
    let max_attempts = attrs.max_attempts;
    let base_delay_ms = attrs.base_delay_ms;
    let backoff_lit = proc_macro2::Literal::f64_unsuffixed(attrs.backoff_multiplier);
    let max_delay_ms = attrs.max_delay_ms;
    let max_in_flight = attrs.max_in_flight;
    let max_enqueue_per_second = attrs.max_enqueue_per_second;
    let idempotency_mode_tokens = match attrs.idempotency_mode.as_deref() {
        Some("none") => {
            quote! { ::core::option::Option::Some(::boson_core::IdempotencyMode::None) }
        }
        Some("lwt") => quote! { ::core::option::Option::Some(::boson_core::IdempotencyMode::Lwt) },
        _ => quote! { ::core::option::Option::None },
    };

    let params: Vec<&PatType> = collect_typed_params(&input.sig.inputs);
    let param_idents: Vec<syn::Ident> = collect_param_idents(&params);

    let struct_name_str = to_pascal_case(&fn_name.to_string()) + "Params";
    let params_struct_name = syn::Ident::new(&struct_name_str, fn_name.span());
    let task_struct_name = syn::Ident::new(
        &to_pascal_case(&task_name.replace('.', "_")),
        fn_name.span(),
    );

    let is_unit_struct = param_idents.is_empty();
    let params_struct = emit_params_struct(is_unit_struct, fn_vis, &params_struct_name, &params);

    let internal_fn_name = syn::Ident::new(&format!("__{fn_name}_impl"), fn_name.span());
    let mut internal_sig = fn_sig.clone();
    internal_sig.ident = internal_fn_name.clone();

    let deserialize_code = quote! {
        let params: #params_struct_name = serde_json::from_value(params_json)
            .map_err(|e| ::boson_core::BosonError::ParamError(e.to_string()))?;
    };

    quote! {
        #params_struct

        /// Task handle (see `send_with`). Task name: see Boson registry entry.
        #fn_vis struct #task_struct_name;

        impl #task_struct_name {
            /// Enqueue this task with the given actor JSON and params.
            #fn_vis async fn send_with(
                actor_json: serde_json::Value,
                params: #params_struct_name,
            ) -> ::boson_core::Result<String> {
                let b = ::boson_runtime::default()
                    .ok_or_else(|| ::boson_core::BosonError::Internal("boson not configured; call boson_runtime::configure()".to_string()))?;
                let params_json = serde_json::to_value(params)
                    .map_err(|e| ::boson_core::BosonError::ParamError(e.to_string()))?;
                b.enqueue(#task_name_lit, actor_json, params_json, None).await
            }
        }

        #(#fn_attrs)*
        #fn_vis #internal_sig #fn_block

        ::quark::inventory::submit! {
            ::boson_runtime::TaskDescriptor::with_policy(
                #task_name_lit,
                |ctx, params_json| {
                    std::boxed::Box::pin(async move {
                        #deserialize_code
                        #internal_fn_name(ctx, #(params.#param_idents),*).await
                            .map_err(|e| ::boson_core::BosonError::Internal(e.to_string()))
                    })
                },
                "{}",
                0u64,
                #priority,
                #pool_lit,
                #max_attempts,
                #base_delay_ms,
                #backoff_lit,
                #max_delay_ms,
                #max_in_flight,
                #max_enqueue_per_second,
                #idempotency_mode_tokens,
            )
        }
    }
}

fn collect_typed_params(
    inputs: &syn::punctuated::Punctuated<FnArg, syn::token::Comma>,
) -> Vec<&PatType> {
    inputs
        .iter()
        .skip_while(|arg| matches!(arg, FnArg::Receiver(_)))
        .skip(1)
        .filter_map(|arg| {
            if let FnArg::Typed(pat_type) = arg {
                Some(pat_type)
            } else {
                None
            }
        })
        .collect()
}

fn collect_param_idents(params: &[&PatType]) -> Vec<syn::Ident> {
    params
        .iter()
        .filter_map(|pat_type| match pat_type.pat.as_ref() {
            Pat::Ident(pat_ident) => Some(pat_ident.ident.clone()),
            _ => None,
        })
        .collect()
}

fn emit_params_struct(
    is_unit_struct: bool,
    fn_vis: &syn::Visibility,
    params_struct_name: &syn::Ident,
    params: &[&PatType],
) -> TokenStream2 {
    if is_unit_struct {
        quote! {
            #[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
            #fn_vis struct #params_struct_name;
        }
    } else {
        quote! {
            #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
            #fn_vis struct #params_struct_name {
                #(#fn_vis #params),*
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn to_pascal_case_handles_empty_segments() {
        assert_eq!(to_pascal_case("already_pascal"), "AlreadyPascal");
        assert_eq!(to_pascal_case("double__underscore"), "DoubleUnderscore");
    }

    #[test]
    fn collect_typed_params_skips_receiver_argument() {
        let sig: syn::Signature = parse_quote! {
            async fn receiver(
                &self,
                ctx: Box<dyn boson_core::ExecutionContext>,
                user_id: String,
                count: i64,
            ) -> boson_core::Result<()>
        };
        let params = collect_typed_params(&sig.inputs);
        assert_eq!(params.len(), 2, "receiver argument should be skipped");
    }

    #[test]
    fn collect_param_idents_ignores_non_ident_patterns() {
        let sig: syn::Signature = parse_quote! {
            async fn mixed(
                ctx: Box<dyn boson_core::ExecutionContext>,
                (user_id,): (String,),
                count: i64,
            ) -> boson_core::Result<()>
        };
        let params = collect_typed_params(&sig.inputs);
        let idents = collect_param_idents(&params);
        assert_eq!(idents.len(), 1, "tuple-pattern arg should be ignored");
        assert_eq!(idents[0].to_string(), "count");
    }
}
