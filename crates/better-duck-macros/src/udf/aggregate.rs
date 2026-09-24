//! `#[duckdb_aggregate]` codegen.
//!
//! Applied to a `mod` containing four free functions — `init`, `update`,
//! `combine`, `finalize` — this generates a `VAggregate` impl (plus a `register`
//! fn) whose state type is inferred from `init`'s return, argument types from
//! `update`, and result type from `finalize`.

use proc_macro2::TokenStream;
use quote::{format_ident, quote, quote_spanned};
use syn::{spanned::Spanned as _, FnArg, Item, ItemFn, ItemMod, ReturnType, Type};

use crate::{attrs::AggregateAttrs, sig::unwrap_result};

/// Finds a free function named `name` among a module's items.
fn find_fn<'a>(
    items: &'a [Item],
    name: &str,
    span: proc_macro2::Span,
) -> syn::Result<&'a ItemFn> {
    items
        .iter()
        .find_map(|it| match it {
            Item::Fn(f) if f.sig.ident == name => Some(f),
            _ => None,
        })
        .ok_or_else(|| {
            syn::Error::new(span, format!("`#[duckdb_aggregate]` module must define a `{name}` fn"))
        })
}

/// The return type of `f`, or an error tagged with `role` if `f` returns nothing.
fn required_return(
    f: &ItemFn,
    role: &str,
) -> syn::Result<Type> {
    match &f.sig.output {
        ReturnType::Default => {
            Err(syn::Error::new_spanned(&f.sig, format!("aggregate `{role}` must return a value")))
        },
        ReturnType::Type(_, ty) => Ok((**ty).clone()),
    }
}

pub(crate) fn expand(
    attrs: AggregateAttrs,
    item: ItemMod,
) -> syn::Result<TokenStream> {
    let span = item.ident.span();
    let Some((_brace, items)) = &item.content else {
        return Err(syn::Error::new_spanned(
            &item,
            "`#[duckdb_aggregate]` must be applied to an inline `mod { … }`, not `mod name;`",
        ));
    };

    let init = find_fn(items, "init", span)?;
    let update = find_fn(items, "update", span)?;
    let _combine = find_fn(items, "combine", span)?;
    let finalize = find_fn(items, "finalize", span)?;

    // State from `init`'s return; result type from `finalize`'s (Result-unwrapped).
    let state_ty = required_return(init, "init")?;
    let (fin_fallible, _fin_err, ret_ty) = unwrap_result(&required_return(finalize, "finalize")?);
    let update_fallible = match &update.sig.output {
        ReturnType::Default => false,
        ReturnType::Type(_, ty) => unwrap_result(ty).0,
    };

    // Value parameter types = `update`'s args after the leading `&mut state`.
    if update.sig.inputs.is_empty() {
        return Err(syn::Error::new_spanned(
            &update.sig,
            "aggregate `update` must take `&mut state` as its first argument",
        ));
    }
    let value_tys: Vec<Type> = update
        .sig
        .inputs
        .iter()
        .skip(1)
        .map(|arg| match arg {
            FnArg::Typed(pt) => Ok((*pt.ty).clone()),
            FnArg::Receiver(r) => {
                Err(syn::Error::new_spanned(r, "aggregate `update` must be a free function"))
            },
        })
        .collect::<syn::Result<_>>()?;

    // <AGG-CODEGEN>
    build(&attrs, &item, items, &state_ty, &ret_ty, &value_tys, update_fallible, fin_fallible)
}

#[allow(clippy::too_many_arguments)]
fn build(
    attrs: &AggregateAttrs,
    item: &ItemMod,
    items: &[Item],
    state_ty: &Type,
    ret_ty: &Type,
    value_tys: &[Type],
    update_fallible: bool,
    fin_fallible: bool,
) -> syn::Result<TokenStream> {
    let mod_ident = &item.ident;
    let vis = &item.vis;
    let attrs_outer = &item.attrs;
    let crate_path =
        attrs.common.crate_path.clone().unwrap_or_else(|| syn::parse_quote!(::better_duck_core));
    let sql_name =
        attrs.common.name.as_ref().map_or_else(|| mod_ident.to_string(), syn::LitStr::value);
    let special = attrs.special_handling;

    let mut col_lets = Vec::with_capacity(value_tys.len());
    let mut reads = Vec::with_capacity(value_tys.len());
    let mut call_args = Vec::with_capacity(value_tys.len());
    let mut param_types = Vec::with_capacity(value_tys.len());
    for (i, vty) in value_tys.iter().enumerate() {
        let col = format_ident!("__col{}", i);
        let a = format_ident!("__a{}", i);
        col_lets.push(quote! { let #col = input.vector(#i)?; });
        reads.push(quote_spanned! {vty.span()=>
            let #a = <#vty as __p::ScalarArg<'_>>::read(&#col, row)?;
        });
        call_args.push(quote! { #a });
        param_types.push(quote_spanned! {vty.span()=> __p::LogicalType::of::<#vty>()? });
    }

    let update_call = if update_fallible {
        quote! { super::update(state, #(#call_args),*).map_err(__p::boxed_error)?; }
    } else {
        quote! { super::update(state, #(#call_args),*); }
    };
    let fin_call = if fin_fallible {
        quote! { let __r = super::finalize(state).map_err(__p::boxed_error)?; }
    } else {
        quote! { let __r = super::finalize(state); }
    };

    // <AGG-QUOTE>
    let methods = quote! {
        type State = #state_ty;
        type Shared = ();

        fn parameters() -> __p::Result<__p::Vec<__p::LogicalType>> {
            __p::StdResult::Ok(__p::Vec::from([#(#param_types),*]))
        }

        fn return_type() -> __p::Result<__p::LogicalType> {
            __p::LogicalType::of::<#ret_ty>()
        }

        fn init() -> Self::State {
            super::init()
        }

        fn update(
            _shared: &(),
            state: &mut Self::State,
            input: &__p::DataChunkHandle,
            row: usize,
        ) -> __p::UdfResult<()> {
            #(#col_lets)*
            #(#reads)*
            #update_call
            __p::StdResult::Ok(())
        }

        fn combine(
            _shared: &(),
            source: &Self::State,
            target: &mut Self::State,
        ) {
            super::combine(target, source);
        }

        fn finalize(
            _shared: &(),
            state: &Self::State,
            output: &mut __p::VectorMut<'_>,
            row: usize,
        ) -> __p::UdfResult<()> {
            #fin_call
            <#ret_ty as __p::ScalarRet>::write(__r, output, row)?;
            __p::StdResult::Ok(())
        }

        fn special_handling() -> bool {
            #special
        }
    };
    // <AGG-ASSEMBLE>
    let expanded = quote! {
        #(#attrs_outer)*
        #vis mod #mod_ident {
            #(#items)*

            #[doc(hidden)]
            #[allow(
                non_camel_case_types,
                non_snake_case,
                missing_docs,
                unused_qualifications,
                clippy::all
            )]
            mod __udf {
                use #crate_path::udf::__private as __p;

                pub struct Udf;

                impl __p::VAggregate for Udf {
                    #methods
                }

                /// Registers this aggregate with `conn`.
                ///
                /// # Errors
                ///
                /// Returns an error if registration fails — see
                /// `Connection::register_aggregate_function`.
                pub fn register(conn: &mut __p::Connection) -> __p::Result<()> {
                    conn.register_aggregate_function::<Udf>(#sql_name)
                }
            }

            #[doc(hidden)]
            pub use __udf::register;
        }
    };
    Ok(expanded)
}
