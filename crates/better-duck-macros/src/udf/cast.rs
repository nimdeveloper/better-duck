//! `#[duckdb_cast]` codegen.

use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::{spanned::Spanned as _, ItemFn, ReturnType};

use crate::{
    attrs::CastAttrs,
    sig::{extract_params, unwrap_result, validate_shape},
};

pub(crate) fn expand(
    attrs: CastAttrs,
    item: ItemFn,
) -> syn::Result<TokenStream> {
    validate_shape(&item)?;

    let params = extract_params(&item)?;
    if params.len() != 1 {
        return Err(syn::Error::new_spanned(
            &item.sig,
            "a cast function must take exactly one argument (the source value)",
        ));
    }
    let ret_ty = match &item.sig.output {
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                &item.sig,
                "a cast function must return the target value",
            ))
        },
        ReturnType::Type(_, ty) => (**ty).clone(),
    };
    let (is_fallible, _error_ty, target_ty) = unwrap_result(&ret_ty);

    let src_ty = params[0].ty.clone();
    let source_lt = quote_spanned! {src_ty.span()=> __p::LogicalType::of::<#src_ty>()? };
    let target_lt = quote_spanned! {target_ty.span()=> __p::LogicalType::of::<#target_ty>()? };

    let fn_ident = item.sig.ident.clone();
    let mod_ident = fn_ident.clone();
    let crate_path = attrs.crate_path.unwrap_or_else(|| syn::parse_quote!(::better_duck_core));
    let implicit_cost = attrs.implicit_cost.unwrap_or(-1);

    let call = quote! { super::#fn_ident(__in) };
    let out_binding = if is_fallible {
        quote! { let __out = #call.map_err(__p::boxed_error)?; }
    } else {
        quote! { let __out = #call; }
    };

    // <CAST-BODY>
    let expanded = quote! {
        #item

        #[doc(hidden)]
        #[allow(non_camel_case_types, non_snake_case, missing_docs, unused_qualifications, clippy::all)]
        mod #mod_ident {
            use #crate_path::udf::__private as __p;

            pub struct Cast;

            impl __p::VCast for Cast {
                type Shared = ();

                fn source_type() -> __p::Result<__p::LogicalType> {
                    __p::StdResult::Ok(#source_lt)
                }

                fn target_type() -> __p::Result<__p::LogicalType> {
                    __p::StdResult::Ok(#target_lt)
                }

                fn implicit_cast_cost() -> i64 {
                    #implicit_cost
                }

                fn cast_row(
                    _shared: &(),
                    input: &__p::VectorRef<'_>,
                    output: &mut __p::VectorMut<'_>,
                    row: usize,
                ) -> __p::UdfResult<()> {
                    let __in: #src_ty = input.get(row)?;
                    #out_binding
                    output.set(row, __out)?;
                    __p::StdResult::Ok(())
                }
            }

            /// Registers this cast with `conn`.
            ///
            /// # Errors
            ///
            /// Returns an error if registration fails — see
            /// `Connection::register_cast_function`.
            pub fn register(conn: &mut __p::Connection) -> __p::Result<()> {
                conn.register_cast_function::<Cast>()
            }
        }
    };
    Ok(expanded)
}

#[cfg(test)]
mod tests {
    use syn::parse_quote;

    use super::expand;
    use crate::attrs::CastAttrs;

    fn compact(tokens: proc_macro2::TokenStream) -> String {
        tokens.to_string().split_whitespace().collect()
    }

    #[test]
    fn rejects_wrong_arity() {
        let err = expand(
            CastAttrs::default(),
            parse_quote!(
                fn c(
                    a: i32,
                    b: i32,
                ) -> i64 {
                    0
                }
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("exactly one argument"), "{err}");
    }

    #[test]
    fn rejects_missing_return() {
        let err = expand(
            CastAttrs::default(),
            parse_quote!(
                fn c(a: i32) {}
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("must return the target value"), "{err}");
    }

    #[test]
    fn expands_infallible_cast() {
        let tokens = compact(
            expand(
                CastAttrs::default(),
                parse_quote!(
                    fn c2f(c: f64) -> f64 {
                        c
                    }
                ),
            )
            .unwrap(),
        );
        assert!(tokens.contains("__p::VCastforCast"), "{tokens}");
        assert!(tokens.contains("register_cast_function::<Cast>"), "{tokens}");
        assert!(tokens.contains("->i64{-1i64}"), "{tokens}");
        assert!(!tokens.contains("map_err(__p::boxed_error)"), "{tokens}");
    }

    #[test]
    fn expands_fallible_cast_with_explicit_cost() {
        let attrs = CastAttrs { crate_path: None, implicit_cost: Some(5) };
        let tokens = compact(
            expand(
                attrs,
                parse_quote!(
                    fn s2i(s: &str) -> Result<i32, String> {
                        todo!()
                    }
                ),
            )
            .unwrap(),
        );
        assert!(tokens.contains("map_err(__p::boxed_error)?"), "{tokens}");
        assert!(tokens.contains("->i64{5i64}"), "{tokens}");
    }
}
