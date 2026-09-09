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

    // Which declared parameters bind as SQL named (keyword) parameters instead
    // of positional ones — validated against the fn's actual parameter names.
    let named_param_names: Vec<String> = attrs
        .named_params
        .as_ref()
        .map(|lits| lits.iter().map(syn::LitStr::value).collect())
        .unwrap_or_default();
    for declared in &named_param_names {
        if !params.iter().any(|p| &p.ident.to_string() == declared) {
            return Err(syn::Error::new_spanned(
                attrs
                    .named_params
                    .as_ref()
                    .and_then(|lits| lits.iter().find(|l| &l.value() == declared))
                    .expect("value came from this list"),
                format!(
                    "`named_params` names `{declared}`, but no parameter with that name exists"
                ),
            ));
        }
    }
    let is_named: Vec<bool> =
        params.iter().map(|p| named_param_names.contains(&p.ident.to_string())).collect();

    let bind_field_decls: Vec<TokenStream> = params
        .iter()
        .zip(&param_fields)
        .map(|(p, field)| {
            let ty = &p.ty;
            quote! { pub #field: #ty }
        })
        .collect();
    let bind_reads: Vec<TokenStream> = {
        let mut positional_idx = 0u64;
        params
            .iter()
            .zip(&param_fields)
            .zip(&is_named)
            .map(|((p, field), &named)| {
                let ty = &p.ty;
                let span = ty.span();
                if named {
                    let name_str = p.ident.to_string();
                    if p.is_option {
                        quote_spanned! {span=>
                            let #field: #ty = bind.get_named_parameter::<#ty>(#name_str)?.flatten();
                        }
                    } else {
                        quote_spanned! {span=>
                            let #field: #ty = bind.get_named_parameter::<#ty>(#name_str)?
                                .ok_or_else(|| ::std::format!(
                                    "missing required named parameter `{}`", #name_str
                                ))?;
                        }
                    }
                } else {
                    let i = positional_idx;
                    positional_idx += 1;
                    quote_spanned! {span=> let #field: #ty = bind.get_parameter(#i as u64)?; }
                }
            })
            .collect()
    };
    let param_logical_types: Vec<TokenStream> = params
        .iter()
        .zip(&is_named)
        .filter(|(_, &named)| !named)
        .map(|(p, _)| {
            let ty = &p.ty;
            let span = ty.span();
            quote_spanned! {span=> __p::LogicalType::of::<#ty>()? }
        })
        .collect();
    let named_param_decls: Vec<TokenStream> = params
        .iter()
        .zip(&is_named)
        .filter(|(_, &named)| named)
        .map(|(p, _)| {
            let ty = &p.ty;
            let span = ty.span();
            let name_str = p.ident.to_string();
            quote_spanned! {span=> (::std::string::String::from(#name_str), __p::LogicalType::of::<#ty>()?) }
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
    let projection_pushdown = attrs.projection_pushdown;
    // Wrapping the call in a `ProjectionGuard` makes `duck_projection!()` work
    // inside the user's fn body — a no-op (empty projection) when the flag
    // isn't set, since nothing ever enters the guard in that case.
    let projection_guard = if projection_pushdown {
        quote! {
            let __proj = init.column_indices();
            // SAFETY: `__proj` outlives the guard — both are local to this
            // call, and the guard is dropped explicitly (see
            // `projection_guard_drop` below) before `__proj` is moved into
            // `TableInitData::with_projection`.
            let __proj_guard = unsafe { __p::ProjectionGuard::enter(&__proj) };
        }
    } else {
        quote! {}
    };
    // Ends `__proj`'s borrow (held by `__proj_guard`, entered above) right
    // after the user's fn body has run — the only place `duck_projection!()`
    // is valid — and before `__proj` itself is moved into `with_projection`
    // a few lines down.
    let projection_guard_drop = if projection_pushdown {
        quote! { drop(__proj_guard); }
    } else {
        quote! {}
    };
    let init_data_expr = if projection_pushdown {
        quote! { __p::TableInitData::new(__iter).with_projection(__proj) }
    } else {
        quote! { __p::TableInitData::new(__iter) }
    };
    // Wrapping the call in a `TableExtraInfoGuard` makes `duck_extra_info!()`
    // work inside the user's fn body — a no-op when `extra_info` wasn't
    // declared, since nothing ever enters the guard in that case (and
    // `duck_extra_info!()` would then correctly panic if called).
    let extra_info_guard = if let Some((ty, _)) = &attrs.extra_info {
        quote! {
            let __extra_info_ptr = init.extra_info::<#ty>().expect(
                "extra info was declared via `extra_info(...)` but is unexpectedly absent"
            ) as *const #ty as *const ();
            // SAFETY: `init.extra_info::<#ty>()` returns a reference kept
            // alive by DuckDB until the catalog entry is dropped, which
            // outlives this call; the guard is dropped (implicitly, at the
            // end of this function) well before that.
            let __extra_info_guard = unsafe {
                __p::TableExtraInfoGuard::enter(__extra_info_ptr.cast())
            };
        }
    } else {
        quote! {}
    };
    let register_call = match &attrs.extra_info {
        // `E` on `register_table_function_with_extra_info` is a free generic
        // (unlike scalar's `state: S::State`, tied to an associated type) —
        // leaving it as `_` let `#init_expr`'s own default integer-literal
        // type (`i32`) win instead of the declared `Type`, silently storing
        // fewer bytes than `duck_extra_info!(Type)` later reads back.
        Some((ty, init_expr)) => quote! {
            conn.register_table_function_with_extra_info::<Udf, #ty>(#sql_name, #init_expr)
        },
        None => quote! { conn.register_table_function::<Udf>(#sql_name) },
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

                fn named_parameters() -> __p::Result<__p::Vec<(::std::string::String, __p::LogicalType)>> {
                    Ok(__p::Vec::from([#(#named_param_decls),*]))
                }

                fn supports_projection_pushdown() -> bool {
                    #projection_pushdown
                }

                fn bind(bind: &__p::BindInfo) -> __p::UdfResult<Self::BindData> {
                    #(#bind_reads)*
                    #(#bind_columns)*
                    Ok(BindData { #(#param_fields),* })
                }

                fn init(init: &__p::InitInfo<Self>) -> __p::UdfResult<Self::InitData> {
                    let bd = init.bind_data();
                    #projection_guard
                    #extra_info_guard
                    #get_iter
                    #boxed_iter
                    #projection_guard_drop
                    Ok(#init_data_expr)
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
                #register_call
            }
        }
    };
    Ok(expanded)
}

#[cfg(test)]
mod tests {
    use syn::{parse_quote, Type};

    use crate::attrs::{CommonAttrs, TableAttrs};

    use super::{expand, extract_iterator_item};

    fn compact(tokens: proc_macro2::TokenStream) -> String {
        tokens.to_string().split_whitespace().collect()
    }

    fn assert_iterator_error(
        ty: Type,
        expected: &str,
    ) {
        let error = extract_iterator_item(&ty).unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }

    #[test]
    fn extracts_iterator_items_regardless_of_bound_order_or_qualification() {
        for ty in [
            parse_quote!(impl Iterator<Item = i64> + Send),
            parse_quote!(impl Send + std::iter::Iterator<Item = (i32, String)>),
        ] {
            let item = extract_iterator_item(&ty).unwrap();
            let actual = quote::quote!(#item).to_string();
            assert!(actual == "i64" || actual == "(i32 , String)");
        }
    }

    #[test]
    fn rejects_invalid_iterator_return_shapes() {
        assert_iterator_error(
            parse_quote!(std::vec::IntoIter<i32>),
            "expected `-> impl Iterator<Item = T> + Send`",
        );
        assert_iterator_error(
            parse_quote!(impl Send),
            "expected `-> impl Iterator<Item = T> + Send`",
        );
        assert_iterator_error(parse_quote!(impl Iterator<Item = i32>), "iterator must be `Send`");
        assert_iterator_error(
            parse_quote!(impl Iterator + Send),
            "expected `-> impl Iterator<Item = T> + Send`",
        );
    }

    #[test]
    fn rejects_a_function_without_a_return_value() {
        let error = expand(
            TableAttrs::default(),
            parse_quote!(
                fn rows() {}
            ),
        )
        .unwrap_err();
        assert!(error.to_string().contains("table functions must return"));
    }

    #[test]
    fn expands_single_column_and_wraps_scalar_iterator_items() {
        let tokens = compact(
            expand(
                TableAttrs::default(),
                parse_quote!(
                    fn numbers() -> impl Iterator<Item = i64> + Send {
                        0..3
                    }
                ),
            )
            .unwrap(),
        );
        assert!(tokens.contains("add_result_column"));
        assert!(tokens.contains("\"numbers\""));
        assert!(tokens.contains("__iter.map(|v|(v,))"));
        assert!(tokens.contains("register_table_function"));
    }

    #[test]
    fn expands_tuple_rows_with_default_column_names() {
        let tokens = compact(
            expand(
                TableAttrs::default(),
                parse_quote!(
                    fn pairs() -> impl Iterator<Item = (i32, String)> + Send {
                        todo!()
                    }
                ),
            )
            .unwrap(),
        );
        assert!(tokens.contains("add_result_column(\"column_0\""), "{tokens}");
        assert!(tokens.contains("add_result_column(\"column_1\""), "{tokens}");
        assert!(!tokens.contains("__iter.map(|v|(v,))"));
    }

    #[test]
    fn rejects_a_column_count_mismatch() {
        let attrs =
            TableAttrs { columns: Some(vec![parse_quote!("only_one")]), ..TableAttrs::default() };
        let error = expand(
            attrs,
            parse_quote!(
                fn pairs() -> impl Iterator<Item = (i32, i32)> + Send {
                    todo!()
                }
            ),
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("`columns` lists 1 names but the row type has 2 columns"));
    }

    #[test]
    fn rejects_unknown_named_parameters() {
        let attrs = TableAttrs {
            named_params: Some(vec![parse_quote!("missing")]),
            ..TableAttrs::default()
        };
        let error = expand(
            attrs,
            parse_quote!(
                fn rows(limit: i64) -> impl Iterator<Item = i64> + Send {
                    todo!()
                }
            ),
        )
        .unwrap_err();
        assert!(error.to_string().contains("no parameter with that name exists"));
    }

    #[test]
    fn expands_named_parameters_and_fallible_iterators() {
        let attrs =
            TableAttrs { named_params: Some(vec![parse_quote!("label")]), ..TableAttrs::default() };
        let tokens = compact(
            expand(
                attrs,
                parse_quote!(
                    fn rows(
                        limit: i64,
                        label: Option<String>,
                    ) -> Result<impl Iterator<Item = i64> + Send, Error> {
                        todo!()
                    }
                ),
            )
            .unwrap(),
        );
        assert!(tokens.contains("LogicalType::of::<i64>()?"));
        assert!(tokens.contains("String::from(\"label\")"));
        assert!(
            tokens.contains("super::rows(bd.p0.clone(),bd.p1.clone()).map_err(__p::boxed_error)?")
        );
    }

    #[test]
    fn honors_name_crate_columns_projection_and_extra_info_options() {
        let attrs = TableAttrs {
            common: CommonAttrs {
                name: Some(parse_quote!("sql_rows")),
                crate_path: Some(parse_quote!(::renamed_duck)),
            },
            columns: Some(vec![parse_quote!("value")]),
            projection_pushdown: true,
            extra_info: Some((parse_quote!(Config), parse_quote!(Config::new()))),
            ..TableAttrs::default()
        };
        let tokens = compact(
            expand(
                attrs,
                parse_quote!(
                    fn rows() -> impl Iterator<Item = i64> + Send {
                        todo!()
                    }
                ),
            )
            .unwrap(),
        );
        for expected in [
            "use::renamed_duck::udf::__privateas__p",
            "add_result_column",
            "\"value\"",
            "fnsupports_projection_pushdown()->bool{true}",
            "typeBindData=BindData",
            "Config::new()",
            "ProjectionGuard::enter(&__proj)",
            "register_table_function",
            "\"sql_rows\"",
        ] {
            assert!(tokens.contains(expected), "missing {expected} in {tokens}");
        }
    }
}
