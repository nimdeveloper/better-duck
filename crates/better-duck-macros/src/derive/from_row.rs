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

#[cfg(test)]
mod tests {
    use super::expand;

    fn compact(input: syn::DeriveInput) -> String {
        expand(input).unwrap().to_string().split_whitespace().collect()
    }

    #[test]
    fn expands_named_struct_with_rename() {
        let tokens = compact(syn::parse_quote! {
            #[duck(rename_all = "snake_case")]
            struct Row { userId: i32, #[duck(rename = "n")] name: String }
        });
        assert!(tokens.contains("FromRowforRow"), "{tokens}");
        assert!(tokens.contains("\"user_id\""), "{tokens}");
        assert!(tokens.contains("\"n\""), "{tokens}");
    }

    #[test]
    fn rejects_non_struct() {
        let err = expand(syn::parse_quote!(enum E { A })).unwrap_err();
        assert!(err.to_string().contains("can only be derived for a struct"), "{err}");
    }

    #[test]
    fn rejects_tuple_struct() {
        let err = expand(syn::parse_quote!(struct T(i32);)).unwrap_err();
        assert!(err.to_string().contains("named fields"), "{err}");
    }
}
