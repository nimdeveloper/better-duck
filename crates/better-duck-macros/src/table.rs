//! `#[duckdb_table_function]` codegen.

use proc_macro2::TokenStream;
use quote::{format_ident, quote, quote_spanned};
use syn::{spanned::Spanned as _, ItemFn, ReturnType, Type, TypeParamBound};

use crate::{
    attrs::TableAttrs,
    sig::{extract_params, unwrap_result, validate_shape},
};

/// Extracts `T` from a return type of the shape `impl Iterator<Item = T> + Send`.
///
/// # Errors
///
/// Returns an error if the type isn't an `impl Trait`, doesn't bound
/// `Iterator<Item = _>`, or is missing a `Send` bound.
fn extract_iterator_item(ty: &Type) -> syn::Result<Type> {
    let Type::ImplTrait(impl_trait) = ty else {
        return Err(syn::Error::new_spanned(
            ty,
            "expected `-> impl Iterator<Item = T> + Send` (optionally inside `Result<_, E>`)",
        ));
    };
    let mut item_ty = None;
    let mut has_send = false;
    for bound in &impl_trait.bounds {
        let TypeParamBound::Trait(trait_bound) = bound else { continue };
        let Some(seg) = trait_bound.path.segments.last() else { continue };
        if seg.ident == "Iterator" {
            if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                for arg in &args.args {
                    if let syn::GenericArgument::AssocType(assoc) = arg {
                        if assoc.ident == "Item" {
                            item_ty = Some(assoc.ty.clone());
                        }
                    }
                }
            }
        } else if seg.ident == "Send" {
            has_send = true;
        }
    }
    let item_ty = item_ty.ok_or_else(|| {
        syn::Error::new_spanned(
            ty,
            "expected `-> impl Iterator<Item = T> + Send` (optionally inside `Result<_, E>`)",
        )
    })?;
    if !has_send {
        return Err(syn::Error::new_spanned(
            ty,
            "the iterator must be `Send`; add `+ Send` to the return type",
        ));
    }
    Ok(item_ty)
}

pub(crate) fn expand(
    attrs: TableAttrs,
    item: ItemFn,
) -> syn::Result<TokenStream> {
    validate_shape(&item)?;

    let ret_ty = match &item.sig.output {
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                &item.sig,
                "table functions must return `impl Iterator<Item = T> + Send`",
            ))
        },
        ReturnType::Type(_, ty) => (**ty).clone(),
    };
    let (is_fallible, _error_ty, iterator_ty) = unwrap_result(&ret_ty);
    let item_ty = extract_iterator_item(&iterator_ty)?;

    // A single-column function may return `Item = T` directly; internally every
    // row is a tuple, so wrap a non-tuple item type in a one-element tuple. This
    // never requires `TableRow` to have a blanket impl for a bare `T`, which
    // would conflict with its tuple impls.
    let (row_ty, elem_types, wrap_item): (Type, Vec<Type>, bool) = match &item_ty {
        Type::Tuple(tuple) => (item_ty.clone(), tuple.elems.iter().cloned().collect(), false),
        other => (syn::parse_quote!((#other,)), vec![other.clone()], true),
    };

    let n_cols = elem_types.len();
    let col_names: Vec<String> = match &attrs.columns {
        Some(names) if names.len() == n_cols => names.iter().map(syn::LitStr::value).collect(),
        Some(names) => {
            return Err(syn::Error::new_spanned(
                &item.sig.output,
                format!(
                    "`columns` lists {} names but the row type has {n_cols} columns",
                    names.len()
                ),
            ))
        },
        None if n_cols == 1 => vec![attrs
            .common
            .name
            .as_ref()
            .map_or_else(|| item.sig.ident.to_string(), syn::LitStr::value)],
        None => (0..n_cols).map(|i| format!("column_{i}")).collect(),
    };

    let fn_ident = item.sig.ident.clone();
    let mod_ident = fn_ident.clone();
    let sql_name = attrs.common.name.map(|l| l.value()).unwrap_or_else(|| fn_ident.to_string());
    let crate_path =
        attrs.common.crate_path.unwrap_or_else(|| syn::parse_quote!(::better_duck_core));

    let params = extract_params(&item)?;
    let param_fields: Vec<syn::Ident> =
        (0..params.len()).map(|i| format_ident!("p{}", i)).collect();

    let bind_field_decls: Vec<TokenStream> = params
        .iter()
        .zip(&param_fields)
        .map(|(p, field)| {
            let ty = &p.ty;
            quote! { pub #field: #ty }
        })
        .collect();
    let bind_reads: Vec<TokenStream> = params
        .iter()
        .zip(&param_fields)
        .enumerate()
        .map(|(i, (p, field))| {
            let ty = &p.ty;
            let span = ty.span();
            quote_spanned! {span=> let #field: #ty = bind.get_parameter(#i as u64)?; }
        })
        .collect();
    let param_logical_types: Vec<TokenStream> = params
        .iter()
        .map(|p| {
            let ty = &p.ty;
            let span = ty.span();
            quote_spanned! {span=> __p::LogicalType::of::<#ty>()? }
        })
        .collect();
    // `init.bind_data()` returns `&BindData`, so each field must be cloned out
    // rather than moved — a non-`Copy` parameter type (e.g. `String`) cannot be
    // moved through a shared reference.
    let call_args: Vec<TokenStream> =
        param_fields.iter().map(|f| quote! { bd.#f.clone() }).collect();

    let bind_columns: Vec<TokenStream> = elem_types
        .iter()
        .zip(&col_names)
        .map(|(ty, name)| {
            let span = ty.span();
            quote_spanned! {span=> bind.add_result_column(#name, &__p::LogicalType::of::<#ty>()?)?; }
        })
        .collect();

    let call = quote! { super::#fn_ident(#(#call_args),*) };
    let get_iter = if is_fallible {
        quote! { let __iter = #call.map_err(__p::boxed_error)?; }
    } else {
        quote! { let __iter = #call; }
    };
    let boxed_iter = if wrap_item {
        quote! {
            let __iter: __p::Box<dyn Iterator<Item = #row_ty> + Send> =
                __p::Box::new(__iter.map(|v| (v,)));
        }
    } else {
        quote! {
            let __iter: __p::Box<dyn Iterator<Item = #row_ty> + Send> = __p::Box::new(__iter);
        }
    };

    let expanded = quote! {
        #item

        #[doc(hidden)]
        #[allow(non_camel_case_types, non_snake_case, missing_docs, unused_qualifications, clippy::all)]
        mod #mod_ident {
            use #crate_path::udf::__private as __p;

            pub struct BindData {
                #(#bind_field_decls),*
            }

            pub struct Udf;

            impl __p::VTab for Udf {
                type BindData = BindData;
                type InitData = __p::TableInitData<#row_ty>;

                fn parameters() -> __p::Result<__p::Vec<__p::LogicalType>> {
                    Ok(__p::Vec::from([#(#param_logical_types),*]))
                }

                fn bind(bind: &__p::BindInfo) -> __p::UdfResult<Self::BindData> {
                    #(#bind_reads)*
                    #(#bind_columns)*
                    Ok(BindData { #(#param_fields),* })
                }

                fn init(init: &__p::InitInfo<Self>) -> __p::UdfResult<Self::InitData> {
                    let bd = init.bind_data();
                    #get_iter
                    #boxed_iter
                    Ok(__p::TableInitData::new(__iter))
                }

                fn func(
                    func: &__p::TableFunctionInfo<Self>,
                    output: &mut __p::DataChunkHandle,
                ) -> __p::UdfResult<()> {
                    __p::run_table_func(func.init_data(), output)
                }
            }

            /// Registers this function with `conn`.
            ///
            /// # Errors
            ///
            /// Returns an error if registration fails — see
            /// `Connection::register_table_function`.
            pub fn register(conn: &mut __p::Connection) -> __p::Result<()> {
                conn.register_table_function::<Udf>(#sql_name)
            }
        }
    };
    Ok(expanded)
}
