//! `#[derive(DuckStruct)]` codegen: maps a Rust struct to a DuckDB `STRUCT` value
//! — `From<T> for DuckValue` (builds a `STRUCT`) and `FromDuckValue for T` (reads one
//! back). Field names follow the same `#[duck(rename = …)]` / `#[duck(rename_all = …)]`
//! rules as `#[derive(FromRow)]`.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, LitStr};

use super::{duck_field_name, parse_duck_container};

pub(crate) fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    let container = parse_duck_container(&input)?;
    let cratep = &container.crate_path;
    let ident = &input.ident;

    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input,
            "`DuckStruct` can only be derived for a struct",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new_spanned(
            &input,
            "`DuckStruct` requires a struct with named fields",
        ));
    };

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let mut inserts = Vec::with_capacity(fields.named.len());
    let mut reads = Vec::with_capacity(fields.named.len());
    for f in &fields.named {
        let fident = f.ident.as_ref().expect("named field has an ident");
        let fty = &f.ty;
        let col = duck_field_name(f, container.rename_all)?;
        let col_lit = LitStr::new(&col, fident.span());
        inserts.push(quote! {
            __m.insert(
                ::std::string::String::from(#col_lit),
                #cratep::types::value::DuckValue::from(__v.#fident),
            );
        });
        reads.push(quote! {
            #fident: {
                let __f = __m.get(#col_lit).ok_or_else(|| {
                    #cratep::error::DuckDBConversionError::ConversionError(
                        ::std::format!("DuckStruct: missing field {:?}", #col_lit))
                })?;
                <#fty as #cratep::FromDuckValue>::from_duck_value(__f)?
            },
        });
    }

    // <DUCKSTRUCT-QUOTE>
    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics ::core::convert::From<#ident #ty_generics>
            for #cratep::types::value::DuckValue #where_clause
        {
            fn from(__v: #ident #ty_generics) -> Self {
                let mut __m = ::std::collections::HashMap::new();
                #(#inserts)*
                #cratep::types::value::DuckValue::Struct(__m)
            }
        }

        #[automatically_derived]
        impl #impl_generics #cratep::FromDuckValue for #ident #ty_generics #where_clause {
            fn from_duck_value(
                __value: &#cratep::types::value::DuckValue,
            ) -> ::core::result::Result<Self, #cratep::error::DuckDBConversionError> {
                match __value {
                    #cratep::types::value::DuckValue::Struct(__m) => {
                        ::core::result::Result::Ok(Self { #(#reads)* })
                    },
                    __other => ::core::result::Result::Err(
                        #cratep::error::DuckDBConversionError::ConversionError(
                            ::std::format!("expected STRUCT, got {:?}", __other)),
                    ),
                }
            }
        }
    })
}
