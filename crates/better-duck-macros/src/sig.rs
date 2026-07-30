//! Function-signature validation and type introspection shared by
//! `#[duckdb_scalar]` and `#[duckdb_table_function]`.

use syn::{FnArg, GenericArgument, ItemFn, Pat, PathArguments, Type};

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
    if let Some(unsafety) = &item.sig.unsafety {
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
