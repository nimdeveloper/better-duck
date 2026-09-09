//! `#[duckdb_scalar]` codegen.

use proc_macro2::TokenStream;
use quote::{format_ident, quote, quote_spanned};
use syn::{spanned::Spanned as _, ItemFn, ReturnType};

use crate::{
    attrs::ScalarAttrs,
    sig::{extract_params, unwrap_result, validate_shape},
};

pub(crate) fn expand(
    attrs: ScalarAttrs,
    item: ItemFn,
) -> syn::Result<TokenStream> {
    validate_shape(&item)?;

    let ret_ty = match &item.sig.output {
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(&item.sig, "scalar functions must return a value"))
        },
        ReturnType::Type(_, ty) => (**ty).clone(),
    };
    let (is_fallible, _error_ty, return_ty) = unwrap_result(&ret_ty);

    let params = extract_params(&item)?;
    let fn_ident = item.sig.ident.clone();
    let mod_ident = fn_ident.clone();
    let sql_name = attrs.common.name.map(|l| l.value()).unwrap_or_else(|| fn_ident.to_string());
    let crate_path =
        attrs.common.crate_path.unwrap_or_else(|| syn::parse_quote!(::better_duck_core));
    let volatile = attrs.volatile;
    let special_handling = params.iter().any(|p| p.is_option);

    let mut col_lets = Vec::with_capacity(params.len());
    let mut read_lets = Vec::with_capacity(params.len());
    let mut call_args = Vec::with_capacity(params.len());
    let mut null_checks = Vec::new();
    let mut param_types = Vec::with_capacity(params.len());

    for (i, p) in params.iter().enumerate() {
        let col_ident = format_ident!("__col{}", i);
        let val_ident = format_ident!("__a{}", i);
        let ty = &p.ty;
        let ty_span = ty.span();

        col_lets.push(quote! { let #col_ident = input.vector(#i)?; });
        read_lets.push(quote_spanned! {ty_span=>
            let #val_ident = <#ty as __p::ScalarArg<'_>>::read(&#col_ident, row)?;
        });
        call_args.push(quote! { #val_ident });
        param_types.push(quote_spanned! {ty_span=> __p::LogicalType::of::<#ty>()? });
        if !p.is_option {
            null_checks.push(quote! { #col_ident.is_null(row) });
        }
    }

    let ret_span = return_ty.span();
    let return_lt = quote_spanned! {ret_span=> __p::LogicalType::of::<#return_ty>()? };

    let call = quote! { super::#fn_ident(#(#call_args),*) };
    let result_binding = if is_fallible {
        quote! { let __r = #call.map_err(__p::boxed_error)?; }
    } else {
        quote! { let __r = #call; }
    };

    let state_ty: syn::Type = match &attrs.state {
        Some((ty, _)) => ty.clone(),
        None => syn::parse_quote!(()),
    };
    // Entering the guard makes `duck_state!()` work inside the user's fn body
    // for the whole row loop — a no-op when no `state` option was declared,
    // since nothing ever enters the guard in that case (and `duck_state!()`
    // would then correctly panic if called, matching the doc'd contract).
    let state_guard = if attrs.state.is_some() {
        quote! {
            // SAFETY: `state` outlives the guard — both are scoped to this
            // call, and the guard is dropped (implicitly, at the end of this
            // function) before `state`'s borrow ends.
            let __state_guard = unsafe {
                __p::ScalarStateGuard::enter((state as *const #state_ty).cast())
            };
        }
    } else {
        quote! {}
    };
    let register_call = match &attrs.state {
        Some((_, init_expr)) => quote! {
            conn.register_scalar_function_with_state::<Udf>(#sql_name, #init_expr)
        },
        None => quote! { conn.register_scalar_function::<Udf>(#sql_name) },
    };

    // Non-`Option` parameters are exempt from special_handling and are not
    // guaranteed to hold a meaningful value on a NULL row (a VARCHAR/BLOB
    // slot's string_t may not even be a valid pointer): skip the call entirely
    // and let DuckDB's NULL-propagation default fill in the output.
    let null_guard = if null_checks.is_empty() {
        quote! {}
    } else {
        quote! {
            if #(#null_checks)||* {
                output.set_null(row);
                continue;
            }
        }
    };

    let expanded = quote! {
        #item

        #[doc(hidden)]
        #[allow(non_camel_case_types, non_snake_case, missing_docs, unused_qualifications, clippy::all)]
        mod #mod_ident {
            use #crate_path::udf::__private as __p;

            pub struct Udf;

            impl __p::VScalar for Udf {
                type State = #state_ty;

                fn signatures() -> __p::Result<__p::Vec<__p::ScalarSignature>> {
                    Ok(__p::Vec::from([__p::ScalarSignature::exact(
                        __p::Vec::from([#(#param_types),*]),
                        #return_lt,
                    )]))
                }

                fn special_handling() -> bool {
                    #special_handling
                }

                fn volatile() -> bool {
                    #volatile
                }

                fn invoke(
                    state: &#state_ty,
                    input: &__p::DataChunkHandle,
                    output: &mut __p::VectorMut<'_>,
                ) -> __p::UdfResult<()> {
                    let _ = state;
                    #state_guard
                    #(#col_lets)*
                    let __n_rows = input.len();
                    for row in 0..__n_rows {
                        #null_guard
                        #(#read_lets)*
                        #result_binding
                        <#return_ty as __p::ScalarRet>::write(__r, output, row)?;
                    }
                    Ok(())
                }
            }

            /// Registers this function with `conn`.
            ///
            /// # Errors
            ///
            /// Returns an error if registration fails — see
            /// `Connection::register_scalar_function`.
            pub fn register(conn: &mut __p::Connection) -> __p::Result<()> {
                #register_call
            }
        }
    };
    Ok(expanded)
}

#[cfg(test)]
mod tests {
    use quote::quote;
    use syn::parse_quote;

    use crate::attrs::{CommonAttrs, ScalarAttrs};

    use super::expand;

    fn compact(tokens: proc_macro2::TokenStream) -> String {
        tokens.to_string().split_whitespace().collect()
    }

    #[test]
    fn rejects_a_function_without_a_return_value() {
        let error = expand(
            ScalarAttrs::default(),
            parse_quote!(
                fn noop() {}
            ),
        )
        .unwrap_err();
        assert!(error.to_string().contains("scalar functions must return a value"));
    }

    #[test]
    fn expands_zero_argument_infallible_function() {
        let tokens = compact(
            expand(
                ScalarAttrs::default(),
                parse_quote!(
                    fn answer() -> i64 {
                        42
                    }
                ),
            )
            .unwrap(),
        );
        assert!(tokens.contains("modanswer"), "{tokens}");
        assert!(tokens.contains("super::answer()"), "{tokens}");
        assert!(tokens.contains("ScalarRet>::write(__r,output,row)?"), "{tokens}");
        assert!(tokens.contains("register_scalar_function"), "{tokens}");
        assert!(tokens.contains("\"answer\""), "{tokens}");
        assert!(!tokens.contains("map_err(__p::boxed_error)"));
        assert!(tokens.contains("fnspecial_handling()->bool{false}"));
    }

    #[test]
    fn expands_fallible_function_with_error_mapping() {
        let tokens = compact(
            expand(
                ScalarAttrs::default(),
                parse_quote!(
                    fn checked(value: i32) -> Result<i64, Error> {
                        todo!()
                    }
                ),
            )
            .unwrap(),
        );
        assert!(tokens.contains("let__r=super::checked(__a0).map_err(__p::boxed_error)?"));
        assert!(tokens.contains("<i64as__p::ScalarRet>::write"));
    }

    #[test]
    fn non_optional_parameters_short_circuit_null_rows() {
        let tokens = compact(
            expand(
                ScalarAttrs::default(),
                parse_quote!(
                    fn add(
                        left: i32,
                        right: i32,
                    ) -> i32 {
                        left + right
                    }
                ),
            )
            .unwrap(),
        );
        assert!(tokens.contains("if__col0.is_null(row)||__col1.is_null(row)"));
        assert!(tokens.contains("output.set_null(row)"));
        assert!(tokens.contains("fnspecial_handling()->bool{false}"));
    }

    #[test]
    fn optional_parameters_enable_special_handling_and_skip_their_null_guard() {
        let tokens = compact(
            expand(
                ScalarAttrs::default(),
                parse_quote!(
                    fn maybe(
                        value: Option<i32>,
                        required: i32,
                    ) -> i32 {
                        todo!()
                    }
                ),
            )
            .unwrap(),
        );
        assert!(tokens.contains("fnspecial_handling()->bool{true}"));
        assert!(tokens.contains("if__col1.is_null(row)"));
        assert!(!tokens.contains("__col0.is_null(row)"));
    }

    #[test]
    fn honors_name_crate_volatility_and_state_options() {
        let attrs = ScalarAttrs {
            common: CommonAttrs {
                name: Some(parse_quote!("sql_counter")),
                crate_path: Some(parse_quote!(::renamed_duck)),
            },
            volatile: true,
            state: Some((parse_quote!(Counter), parse_quote!(Counter::new()))),
        };
        let tokens = compact(
            expand(
                attrs,
                parse_quote!(
                    fn counter() -> i64 {
                        duck_state!().next()
                    }
                ),
            )
            .unwrap(),
        );
        for expected in [
            "use::renamed_duck::udf::__privateas__p",
            "typeState=Counter",
            "Counter::new()",
            "fnvolatile()->bool{true}",
            "register_scalar_function_with_state::<Udf>",
            "\"sql_counter\"",
            "ScalarStateGuard::enter",
        ] {
            assert!(tokens.contains(expected), "missing {expected} in {tokens}");
        }
    }

    #[test]
    fn generated_module_keeps_the_original_function() {
        let item = parse_quote!(
            fn identity(value: i32) -> i32 {
                value
            }
        );
        let tokens = expand(ScalarAttrs::default(), item).unwrap();
        assert!(quote!(#tokens).to_string().contains("fn identity"));
    }
}
