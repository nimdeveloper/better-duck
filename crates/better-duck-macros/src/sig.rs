//! Function-signature validation and type introspection shared by
//! `#[duckdb_scalar]` and `#[duckdb_table_function]`.

use syn::{FnArg, GenericArgument, ItemFn, Pat, PathArguments, Safety, Type};

/// One parameter of a user-annotated function.
pub(crate) struct Param {
    /// Unused by `#[duckdb_scalar]` (parameters are matched positionally);
    /// used by `#[duckdb_table_function]` to match `named_params(...)` entries
    /// against actual parameter names.
    pub(crate) ident: syn::Ident,
    /// The parameter's full declared type, `Option<T>` wrapper included if
    /// present — both `better_duck_core`'s `ScalarArg` and `DuckLogicalType`
    /// traits have a blanket `Option<T>` impl, so the wrapper never needs to be
    /// stripped for codegen purposes.
    pub(crate) ty: Type,
    /// Whether `ty` is syntactically `Option<...>` — used only to decide
    /// whether this parameter is exempt from the pre-call null-skip guard and
    /// whether the function needs `special_handling()`.
    pub(crate) is_option: bool,
}

/// Rejects `async`, `unsafe`, non-Rust ABI, generics/`where`, and a `self`
/// receiver — none of which make sense for a DuckDB UDF, which is registered
/// as a bare, monomorphic function pointer.
pub(crate) fn validate_shape(item: &ItemFn) -> syn::Result<()> {
    if let Some(asyncness) = &item.sig.asyncness {
        return Err(syn::Error::new_spanned(asyncness, "DuckDB UDFs cannot be `async`"));
    }
    if let Safety::Unsafe(unsafety) = &item.sig.safety {
        return Err(syn::Error::new_spanned(unsafety, "DuckDB UDFs must be a safe Rust `fn`"));
    }
    if let Some(abi) = &item.sig.abi {
        return Err(syn::Error::new_spanned(abi, "DuckDB UDFs must not specify an ABI"));
    }
    if !item.sig.generics.params.is_empty() || item.sig.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(&item.sig.generics, "DuckDB UDFs must not be generic"));
    }
    for arg in &item.sig.inputs {
        if let FnArg::Receiver(recv) = arg {
            return Err(syn::Error::new_spanned(
                recv,
                "DuckDB UDFs must be a free function, not a method",
            ));
        }
    }
    Ok(())
}

/// Extracts the parameter list, in declaration order.
///
/// Each parameter's pattern must be a simple identifier (no destructuring) —
/// the generated code needs one name per parameter to read into.
pub(crate) fn extract_params(item: &ItemFn) -> syn::Result<Vec<Param>> {
    item.sig
        .inputs
        .iter()
        .map(|arg| {
            let FnArg::Typed(pat_ty) = arg else {
                unreachable!("receivers are rejected by validate_shape before this runs")
            };
            let ident = match &*pat_ty.pat {
                Pat::Ident(pi) => pi.ident.clone(),
                other => {
                    return Err(syn::Error::new_spanned(
                        other,
                        "DuckDB UDF parameters must be a simple identifier, not a pattern",
                    ))
                },
            };
            let ty = (*pat_ty.ty).clone();
            let is_option = is_option_type(&ty);
            Ok(Param { ident, ty, is_option })
        })
        .collect()
}

/// Returns `true` if `ty` is syntactically `Option<...>`.
pub(crate) fn is_option_type(ty: &Type) -> bool {
    let Type::Path(type_path) = ty else { return false };
    type_path.path.segments.last().is_some_and(|seg| seg.ident == "Option")
}

/// If `ty` is syntactically `Result<T, E>`, returns `(true, Some(E), T)`;
/// otherwise `(false, None, ty)` unchanged.
pub(crate) fn unwrap_result(ty: &Type) -> (bool, Option<Type>, Type) {
    if let Type::Path(type_path) = ty {
        if let Some(seg) = type_path.path.segments.last() {
            if seg.ident == "Result" {
                if let PathArguments::AngleBracketed(args) = &seg.arguments {
                    let mut iter = args.args.iter();
                    if let (Some(GenericArgument::Type(ok)), Some(GenericArgument::Type(err))) =
                        (iter.next(), iter.next())
                    {
                        return (true, Some(err.clone()), ok.clone());
                    }
                }
            }
        }
    }
    (false, None, ty.clone())
}

#[cfg(test)]
mod tests {
    use quote::quote;
    use syn::{parse_quote, ItemFn, Type};

    use super::{extract_params, is_option_type, unwrap_result, validate_shape};

    fn assert_shape_error(
        item: ItemFn,
        expected: &str,
    ) {
        let error = validate_shape(&item).unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }

    #[test]
    fn validate_shape_accepts_a_free_monomorphic_function() {
        let item = parse_quote!(
            fn add(
                left: i32,
                right: i32,
            ) -> i32 {
                left + right
            }
        );
        validate_shape(&item).unwrap();
    }

    #[test]
    fn validate_shape_rejects_unsupported_signatures() {
        assert_shape_error(
            parse_quote!(
                async fn f() {}
            ),
            "cannot be `async`",
        );
        assert_shape_error(
            parse_quote!(
                unsafe fn f() {}
            ),
            "must be a safe Rust `fn`",
        );
        assert_shape_error(
            parse_quote!(
                extern "C" fn f() {}
            ),
            "must not specify an ABI",
        );
        assert_shape_error(
            parse_quote!(
                extern "Rust" fn f() {}
            ),
            "must not specify an ABI",
        );
        assert_shape_error(
            parse_quote!(
                fn f<T>() {}
            ),
            "must not be generic",
        );
        assert_shape_error(
            parse_quote!(
                fn f()
                where
                    Self: Sized,
                {
                }
            ),
            "must not be generic",
        );
        assert_shape_error(
            parse_quote!(
                fn f(&self) {}
            ),
            "must be a free function, not a method",
        );
    }

    #[test]
    fn extract_params_preserves_names_types_order_and_option_flags() {
        let item: ItemFn = parse_quote!(
            fn f(
                first: i32,
                second: std::option::Option<String>,
                third: &str,
            ) {
            }
        );
        let params = extract_params(&item).unwrap();
        assert_eq!(params.len(), 3);
        assert_eq!(params[0].ident, "first");
        let first_ty = &params[0].ty;
        assert_eq!(quote!(#first_ty).to_string(), "i32");
        assert!(!params[0].is_option);
        assert_eq!(params[1].ident, "second");
        let second_ty = &params[1].ty;
        assert_eq!(quote!(#second_ty).to_string(), "std :: option :: Option < String >");
        assert!(params[1].is_option);
        assert_eq!(params[2].ident, "third");
        let third_ty = &params[2].ty;
        assert_eq!(quote!(#third_ty).to_string(), "& str");
    }

    #[test]
    fn extract_params_accepts_identifier_binding_modifiers() {
        let item: ItemFn = parse_quote!(
            fn f(
                mut value: i32,
                ref name: String,
            ) {
            }
        );
        let params = extract_params(&item).unwrap();
        assert_eq!(params[0].ident, "value");
        assert_eq!(params[1].ident, "name");
    }

    #[test]
    fn extract_params_rejects_patterns() {
        for item in [
            parse_quote!(
                fn f((left, right): (i32, i32)) {}
            ),
            parse_quote!(
                fn f(Point { x, y }: Point) {}
            ),
            parse_quote!(
                fn f(_: i32) {}
            ),
        ] {
            let error = extract_params(&item).err().expect("pattern must be rejected");
            assert!(error.to_string().contains("must be a simple identifier"));
        }
    }

    #[test]
    fn option_detection_is_syntactic_and_uses_the_final_path_segment() {
        for ty in [
            parse_quote!(Option<i32>),
            parse_quote!(std::option::Option<i32>),
            parse_quote!(custom::Option<i32>),
        ] {
            assert!(is_option_type(&ty));
        }
        for ty in [
            parse_quote!(&Option<i32>),
            parse_quote!((Option<i32>)),
            parse_quote!((Option<i32>,)),
            parse_quote!(Maybe<i32>),
        ] {
            assert!(!is_option_type(&ty));
        }
    }

    #[test]
    fn unwrap_result_accepts_plain_and_qualified_results() {
        for ty in [parse_quote!(Result<i32, Error>), parse_quote!(std::result::Result<i32, Error>)]
        {
            let (fallible, error, ok) = unwrap_result(&ty);
            assert!(fallible);
            assert_eq!(quote!(#ok).to_string(), "i32");
            let error = error.expect("Result must expose its error type");
            assert_eq!(quote!(#error).to_string(), "Error");
        }
    }

    #[test]
    fn unwrap_result_leaves_other_shapes_unchanged() {
        for ty in [parse_quote!(i32), parse_quote!((Result<i32, Error>)), parse_quote!(Result<i32>)]
        {
            let expected = quote!(#ty).to_string();
            let (fallible, error, output): (bool, Option<Type>, Type) = unwrap_result(&ty);
            assert!(!fallible);
            assert!(error.is_none());
            assert_eq!(quote!(#output).to_string(), expected);
        }
    }
}
