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
