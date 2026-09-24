//! `#[derive(FromRow)]` codegen: reads each struct field from a query row by
//! column name via `better_duck_core::FromDuckValue`.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, LitStr};

use super::{duck_field_name, parse_duck_container};

pub(crate) fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    let container = parse_duck_container(&input)?;
    let cratep = &container.crate_path;
    let ident = &input.ident;

    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(&input, "`FromRow` can only be derived for a struct"));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new_spanned(
            &input,
            "`FromRow` requires a struct with named fields",
        ));
    };

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let mut field_inits = Vec::with_capacity(fields.named.len());
    for f in &fields.named {
        let fident = f.ident.as_ref().expect("named field has an ident");
        let fty = &f.ty;
        let col = duck_field_name(f, container.rename_all)?;
        let col_lit = LitStr::new(&col, fident.span());
        field_inits.push(quote! {
            #fident: {
                let __v = row.get(#col_lit).ok_or_else(|| {
                    #cratep::error::DuckDBConversionError::ConversionError(
                        ::std::format!("FromRow: missing column {:?}", #col_lit))
                })?;
                <#fty as #cratep::FromDuckValue>::from_duck_value(__v)?
            },
        });
    }

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #cratep::FromRow for #ident #ty_generics #where_clause {
            fn from_row(
                row: &#cratep::DuckRow,
            ) -> ::core::result::Result<Self, #cratep::error::DuckDBConversionError> {
                ::core::result::Result::Ok(Self { #(#field_inits)* })
            }
        }
    })
}
