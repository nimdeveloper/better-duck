//! `#[derive(FromRow)]` codegen: reads each struct field from a query row by
//! column name via `better_duck_core::FromDuckValue`.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, LitStr, Path};

use super::RenameRule;

/// Container-level `#[duck(...)]` options.
struct ContainerOpts {
    crate_path: Path,
    rename_all: Option<RenameRule>,
}

fn parse_container(input: &DeriveInput) -> syn::Result<ContainerOpts> {
    let mut crate_path: Path = syn::parse_quote!(::better_duck_core);
    let mut rename_all = None;
    for attr in &input.attrs {
        if !attr.path().is_ident("duck") {
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
                Err(meta.error("unknown `duck` container option; expected `crate` or `rename_all`"))
            }
        })?;
    }
    Ok(ContainerOpts { crate_path, rename_all })
}

/// Reads a field's `#[duck(rename = "...")]`, if present.
fn field_rename(field: &syn::Field) -> syn::Result<Option<String>> {
    let mut rename = None;
    for attr in &field.attrs {
        if !attr.path().is_ident("duck") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let s: LitStr = meta.value()?.parse()?;
                rename = Some(s.value());
                Ok(())
            } else {
                Err(meta.error("unknown `duck` field option; expected `rename`"))
            }
        })?;
    }
    Ok(rename)
}

pub(crate) fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    let container = parse_container(&input)?;
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
        let col = match field_rename(f)? {
            Some(name) => name,
            None => container
                .rename_all
                .map_or_else(|| fident.to_string(), |r| r.apply(&fident.to_string())),
        };
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
