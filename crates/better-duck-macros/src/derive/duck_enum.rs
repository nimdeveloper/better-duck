//! `#[derive(DuckEnum)]` codegen: maps a Rust unit enum to a DuckDB `ENUM`.
//!
//! Generates conversions in both directions — `From<T> for DuckValue`,
//! `FromDuckValue for T`, and `AppendAble for T` (binding a value by its label,
//! which DuckDB casts to the target `ENUM`). Each variant's label defaults to its
//! identifier and can be overridden per-variant (`#[duck_enum(rename = "…")]`) or
//! for the whole enum (`#[duck_enum(rename_all = "…")]`).

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, LitStr, Path};

use super::RenameRule;

/// Container-level `#[duck_enum(...)]` options.
struct ContainerOpts {
    crate_path: Path,
    rename_all: Option<RenameRule>,
}

fn parse_container(input: &DeriveInput) -> syn::Result<ContainerOpts> {
    let mut crate_path: Path = syn::parse_quote!(::better_duck_core);
    let mut rename_all = None;
    for attr in &input.attrs {
        if !attr.path().is_ident("duck_enum") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("crate") {
                crate_path = meta.value()?.parse()?;
                Ok(())
            } else if meta.path.is_ident("rename_all") {
                let s: LitStr = meta.value()?.parse()?;
                rename_all = Some(RenameRule::from_name(&s.value()).map_err(|e| meta.error(e))?);
                Ok(())
            } else {
                Err(meta.error(
                    "unknown `duck_enum` container option; expected `crate` or `rename_all`",
                ))
            }
        })?;
    }
    Ok(ContainerOpts { crate_path, rename_all })
}

/// Reads a variant's `#[duck_enum(rename = "...")]`, if present.
fn variant_rename(variant: &syn::Variant) -> syn::Result<Option<String>> {
    let mut rename = None;
    for attr in &variant.attrs {
        if !attr.path().is_ident("duck_enum") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let s: LitStr = meta.value()?.parse()?;
                rename = Some(s.value());
                Ok(())
            } else {
                Err(meta.error("unknown `duck_enum` variant option; expected `rename`"))
            }
        })?;
    }
    Ok(rename)
}

// <DUCKENUM-EXPAND>

pub(crate) fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    let container = parse_container(&input)?;
    let cratep = &container.crate_path;
    let ident = &input.ident;

    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new_spanned(&input, "`DuckEnum` can only be derived for an enum"));
    };

    let mut variants = Vec::with_capacity(data.variants.len());
    for v in &data.variants {
        if !matches!(v.fields, Fields::Unit) {
            return Err(syn::Error::new_spanned(
                v,
                "`DuckEnum` variants must be unit variants (no fields)",
            ));
        }
        let label = match variant_rename(v)? {
            Some(name) => name,
            None => container
                .rename_all
                .map_or_else(|| v.ident.to_string(), |r| r.apply(&v.ident.to_string())),
        };
        variants.push((v.ident.clone(), LitStr::new(&label, v.ident.span())));
    }
    if variants.is_empty() {
        return Err(syn::Error::new_spanned(&input, "`DuckEnum` requires at least one variant"));
    }

    let label_arms = variants.iter().map(|(id, lit)| quote! { #ident::#id => #lit });
    let from_label_arms = variants
        .iter()
        .map(|(id, lit)| quote! { #lit => ::core::option::Option::Some(#ident::#id) });
    let dict_entries = variants.iter().map(|(_, lit)| quote! { ::std::string::String::from(#lit) });

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // <DUCKENUM-QUOTE>
    let inherent_and_from = quote! {
        #[doc(hidden)]
        #[allow(clippy::all)]
        impl #impl_generics #ident #ty_generics #where_clause {
            fn __duck_label(&self) -> &'static str {
                match self { #(#label_arms),* }
            }
            fn __duck_from_label(__s: &str) -> ::core::option::Option<Self> {
                match __s {
                    #(#from_label_arms,)*
                    _ => ::core::option::Option::None,
                }
            }
            fn __duck_dictionary() -> ::std::sync::Arc<[::std::string::String]> {
                ::std::sync::Arc::from(::std::vec![#(#dict_entries),*])
            }
        }

        #[automatically_derived]
        impl #impl_generics ::core::convert::From<#ident #ty_generics>
            for #cratep::types::value::DuckValue #where_clause
        {
            fn from(__v: #ident #ty_generics) -> Self {
                #cratep::types::value::DuckValue::Enum(
                    #cratep::types::DuckEnum::from_label(
                        <#ident #ty_generics>::__duck_dictionary(),
                        __v.__duck_label(),
                    )
                    .expect("derived DuckEnum label is always present in its own dictionary")
                )
            }
        }
    };
    // <DUCKENUM-QUOTE2>
    let traits = quote! {
        #[automatically_derived]
        impl #impl_generics #cratep::FromDuckValue for #ident #ty_generics #where_clause {
            fn from_duck_value(
                __v: &#cratep::types::value::DuckValue,
            ) -> ::core::result::Result<Self, #cratep::error::DuckDBConversionError> {
                let __label: &str = match __v {
                    #cratep::types::value::DuckValue::Enum(__e) => __e.label(),
                    #cratep::types::value::DuckValue::Text(__s) => __s.as_str(),
                    __other => {
                        return ::core::result::Result::Err(
                            #cratep::error::DuckDBConversionError::ConversionError(
                                ::std::format!(
                                    "cannot read {:?} as enum {}",
                                    __other,
                                    ::core::stringify!(#ident)
                                ),
                            ),
                        );
                    },
                };
                <#ident #ty_generics>::__duck_from_label(__label).ok_or_else(|| {
                    #cratep::error::DuckDBConversionError::ConversionError(::std::format!(
                        "unknown enum label {:?} for {}",
                        __label,
                        ::core::stringify!(#ident)
                    ))
                })
            }
        }

        #[automatically_derived]
        impl #impl_generics #cratep::AppendAble for #ident #ty_generics #where_clause {
            fn stmt_append(
                &mut self,
                __idx: u64,
                __stmt: #cratep::ffi::duckdb_prepared_statement,
            ) -> #cratep::error::Result<()> {
                let mut __s = ::std::string::String::from(self.__duck_label());
                #cratep::AppendAble::stmt_append(&mut __s, __idx, __stmt)
            }
            fn appender_append(
                &mut self,
                __appender: #cratep::ffi::duckdb_appender,
            ) -> #cratep::error::Result<()> {
                let mut __s = ::std::string::String::from(self.__duck_label());
                #cratep::AppendAble::appender_append(&mut __s, __appender)
            }
        }
    };
    Ok(quote! {
        #inherent_and_from
        #traits
    })
}
