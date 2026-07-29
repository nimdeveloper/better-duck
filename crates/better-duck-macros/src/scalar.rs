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
                type State = ();

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
                    _state: &(),
                    input: &__p::DataChunkHandle,
                    output: &mut __p::VectorMut<'_>,
                ) -> __p::UdfResult<()> {
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
                conn.register_scalar_function::<Udf>(#sql_name)
            }
        }
    };
    Ok(expanded)
}
