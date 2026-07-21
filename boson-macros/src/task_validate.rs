use syn::{
    AngleBracketedGenericArguments, FnArg, GenericArgument, Pat, PathArguments, ReturnType,
    Signature, Type, TypePath, TypeTraitObject, TypeTuple,
};

pub fn validate_signature(sig: &Signature) -> syn::Result<()> {
    if sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            sig,
            "#[boson::task] function must be async",
        ));
    }

    validate_first_parameter(sig)?;
    validate_return_type(sig)?;
    Ok(())
}

fn validate_first_parameter(sig: &Signature) -> syn::Result<()> {
    let first_param = sig.inputs.first().ok_or_else(|| {
        syn::Error::new_spanned(
            sig,
            "#[boson::task] function must accept Box<dyn ExecutionContext> as the first parameter",
        )
    })?;

    let FnArg::Typed(pat_type) = first_param else {
        return Err(syn::Error::new_spanned(
            first_param,
            "#[boson::task] methods are not supported; use a free function",
        ));
    };

    if !matches!(pat_type.pat.as_ref(), Pat::Ident(_)) {
        return Err(syn::Error::new_spanned(
            &pat_type.pat,
            "#[boson::task] first parameter must be a named ExecutionContext binding",
        ));
    }

    if !is_execution_context_param(pat_type.ty.as_ref()) {
        return Err(syn::Error::new_spanned(
            &pat_type.ty,
            "#[boson::task] first parameter must be Box<dyn ExecutionContext>",
        ));
    }

    Ok(())
}

fn validate_return_type(sig: &Signature) -> syn::Result<()> {
    match &sig.output {
        ReturnType::Type(_, ty) if is_result_unit(ty.as_ref()) => Ok(()),
        _ => Err(syn::Error::new_spanned(
            sig,
            "#[boson::task] return type must be Result<()> (for example boson_core::Result<()>)",
        )),
    }
}

fn is_execution_context_param(ty: &Type) -> bool {
    match ty {
        Type::Path(type_path) => is_box_dyn_execution_context(type_path),
        Type::TraitObject(type_trait_object) => {
            trait_object_has_execution_context(type_trait_object)
        }
        _ => false,
    }
}

fn is_box_dyn_execution_context(type_path: &TypePath) -> bool {
    let Some(segment) = type_path.path.segments.last() else {
        return false;
    };
    if segment.ident != "Box" {
        return false;
    }
    let PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }) =
        &segment.arguments
    else {
        return false;
    };
    let Some(GenericArgument::Type(inner)) = args.first() else {
        return false;
    };
    match inner {
        Type::TraitObject(type_trait_object) => {
            trait_object_has_execution_context(type_trait_object)
        }
        _ => false,
    }
}

fn trait_object_has_execution_context(type_trait_object: &TypeTraitObject) -> bool {
    type_trait_object.bounds.iter().any(|bound| {
        if let syn::TypeParamBound::Trait(trait_bound) = bound {
            trait_bound
                .path
                .segments
                .last()
                .is_some_and(|segment| segment.ident == "ExecutionContext")
        } else {
            false
        }
    })
}

fn is_result_unit(ty: &Type) -> bool {
    let Type::Path(TypePath { qself: None, path }) = ty else {
        return false;
    };

    let Some(last_segment) = path.segments.last() else {
        return false;
    };

    if last_segment.ident != "Result" {
        return false;
    }

    let PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }) =
        &last_segment.arguments
    else {
        return false;
    };

    if args.len() != 1 {
        return false;
    }

    matches!(
        args.first(),
        Some(GenericArgument::Type(Type::Tuple(TypeTuple { elems, .. }))) if elems.is_empty()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn validate_signature_rejects_missing_first_param() {
        let sig: Signature = parse_quote! {
            async fn missing_first() -> boson_core::Result<()>
        };
        let error = validate_signature(&sig).expect_err("missing first param must fail");
        assert!(error
            .to_string()
            .contains("must accept Box<dyn ExecutionContext>"));
    }

    #[test]
    fn validate_signature_rejects_receiver_methods() {
        let sig: Signature = parse_quote! {
            async fn receiver(&self) -> boson_core::Result<()>
        };
        let error = validate_signature(&sig).expect_err("receiver methods must fail");
        assert!(error.to_string().contains("methods are not supported"));
    }

    #[test]
    fn validate_signature_rejects_destructured_first_param() {
        let sig: Signature = parse_quote! {
            async fn destructured((ctx,): (Box<dyn boson_core::ExecutionContext>,)) -> boson_core::Result<()>
        };
        let error = validate_signature(&sig).expect_err("destructured first param must fail");
        assert!(error
            .to_string()
            .contains("first parameter must be a named ExecutionContext binding"));
    }

    #[test]
    fn validate_signature_rejects_non_path_return_type() {
        let sig: Signature = parse_quote! {
            async fn non_path_return(ctx: Box<dyn boson_core::ExecutionContext>) -> &str
        };
        let error = validate_signature(&sig).expect_err("non-path returns must fail");
        assert!(error.to_string().contains("return type must be Result<()>"));
    }

    #[test]
    fn validate_signature_accepts_box_dyn_execution_context() {
        let sig: Signature = parse_quote! {
            async fn ok(ctx: Box<dyn boson_core::ExecutionContext>) -> boson_core::Result<()>
        };
        validate_signature(&sig).expect("valid signature");
    }
}
