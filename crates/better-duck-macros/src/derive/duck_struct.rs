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
    #[cfg(feature = "diesel")]
    let diesel_impls = diesel_emission(cratep, ident, &impl_generics, &ty_generics, where_clause);
    #[cfg(not(feature = "diesel"))]
    let diesel_impls = quote! {};

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

        #diesel_impls
    })
}

/// When the macros crate's `diesel` feature is on, emits `FromSql`/`ToSql` for the
/// derived struct at `better_duck_diesel::sql_types::DuckStruct`, routing through
/// the struct's `From<T> for DuckValue` / `FromDuckValue` impls above.
#[cfg(feature = "diesel")]
use syn::Path;
#[cfg(feature = "diesel")]
fn diesel_emission(
    cratep: &Path,
    ident: &syn::Ident,
    impl_generics: &syn::ImplGenerics<'_>,
    ty_generics: &syn::TypeGenerics<'_>,
    where_clause: Option<&syn::WhereClause>,
) -> TokenStream {
    quote! {
        #[automatically_derived]
        impl #impl_generics ::diesel::serialize::ToSql<
            ::better_duck_diesel::sql_types::DuckStruct,
            ::better_duck_diesel::backend::DuckDb,
        > for #ident #ty_generics #where_clause
        where
            #ident #ty_generics: ::core::clone::Clone,
        {
            fn to_sql<'__b>(
                &'__b self,
                __out: &mut ::diesel::serialize::Output<
                    '__b, '_, ::better_duck_diesel::backend::DuckDb,
                >,
            ) -> ::diesel::serialize::Result {
                let __dv = #cratep::types::value::DuckValue::from(::core::clone::Clone::clone(self));
                __out.set_value(#cratep::types::value_ref::DuckValueRef::from(__dv));
                ::core::result::Result::Ok(::diesel::serialize::IsNull::No)
            }
        }

        #[automatically_derived]
        impl #impl_generics ::diesel::deserialize::FromSql<
            ::better_duck_diesel::sql_types::DuckStruct,
            ::better_duck_diesel::backend::DuckDb,
        > for #ident #ty_generics #where_clause {
            fn from_sql(
                __val: #cratep::types::value_ref::DuckValueRef<'_>,
            ) -> ::diesel::deserialize::Result<Self> {
                let __dv = #cratep::types::value::DuckValue::from(&__val);
                <Self as #cratep::FromDuckValue>::from_duck_value(&__dv)
                    .map_err(|__e| ::std::convert::Into::into(::std::format!("{:?}", __e)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::expand;

    fn compact(input: syn::DeriveInput) -> String {
        expand(input).unwrap().to_string().split_whitespace().collect()
    }

    #[test]
    fn expands_from_and_fromduckvalue_for_named_struct() {
        let tokens = compact(syn::parse_quote! {
            #[duck(rename_all = "camelCase")]
            struct Point { x_axis: i32, y_axis: i32 }
        });
        assert!(tokens.contains("DuckValue::Struct"), "{tokens}");
        assert!(tokens.contains("FromDuckValueforPoint"), "{tokens}");
        assert!(tokens.contains("\"xAxis\""), "{tokens}");
    }

    #[test]
    fn rejects_non_struct_and_tuple() {
        assert!(expand(syn::parse_quote!(
            enum E {
                A,
            }
        ))
        .unwrap_err()
        .to_string()
        .contains("can only be derived for a struct"));
        assert!(expand(syn::parse_quote!(
            struct T(i32);
        ))
        .unwrap_err()
        .to_string()
        .contains("named fields"));
    }
}
