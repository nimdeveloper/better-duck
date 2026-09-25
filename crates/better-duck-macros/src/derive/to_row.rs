//! `#[derive(ToRow)]` codegen: generates an `AppendAble` impl for a struct so it
//! can be appended as one appender row (`Appender::append`) or bound as
//! consecutive statement parameters (`$1`, `$2`, …), each field in declaration
//! order. Every field type must itself be `AppendAble`.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields};

use super::parse_duck_container;

pub(crate) fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    let container = parse_duck_container(&input)?;
    let cratep = &container.crate_path;
    let ident = &input.ident;

    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(&input, "`ToRow` can only be derived for a struct"));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new_spanned(&input, "`ToRow` requires a struct with named fields"));
    };

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let mut appender_calls = Vec::with_capacity(fields.named.len());
    let mut stmt_calls = Vec::with_capacity(fields.named.len());
    for (i, f) in fields.named.iter().enumerate() {
        let fident = f.ident.as_ref().expect("named field has an ident");
        appender_calls.push(quote! {
            #cratep::AppendAble::appender_append(&mut self.#fident, __appender)?;
        });
        // First parameter is at the caller's base `__idx`; the rest follow it.
        let idx_expr = if i == 0 {
            quote! { __idx }
        } else {
            let offset = i as u64;
            quote! { __idx + #offset }
        };
        stmt_calls.push(quote! {
            #cratep::AppendAble::stmt_append(&mut self.#fident, #idx_expr, __stmt)?;
        });
    }

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #cratep::AppendAble for #ident #ty_generics #where_clause {
            fn appender_append(
                &mut self,
                __appender: #cratep::ffi::duckdb_appender,
            ) -> #cratep::error::Result<()> {
                #(#appender_calls)*
                ::core::result::Result::Ok(())
            }
            fn stmt_append(
                &mut self,
                __idx: u64,
                __stmt: #cratep::ffi::duckdb_prepared_statement,
            ) -> #cratep::error::Result<()> {
                #(#stmt_calls)*
                ::core::result::Result::Ok(())
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
    fn expands_appendable_for_named_struct() {
        let tokens = compact(syn::parse_quote! {
            struct Pt { x: i32, y: i32 }
        });
        assert!(tokens.contains("implAppendAbleforPt") || tokens.contains("AppendAbleforPt"), "{tokens}");
        // First field uses the base index; the second is offset by 1.
        assert!(tokens.contains("__idx+1u64"), "{tokens}");
        assert!(tokens.contains("appender_append"), "{tokens}");
    }

    #[test]
    fn honors_crate_override() {
        let tokens = compact(syn::parse_quote! {
            #[duck(crate = ::my_core)]
            struct One { a: i32 }
        });
        assert!(tokens.contains("::my_core::AppendAble"), "{tokens}");
    }

    #[test]
    fn rejects_non_struct_and_tuple() {
        assert!(expand(syn::parse_quote!(enum E { A }))
            .unwrap_err()
            .to_string()
            .contains("can only be derived for a struct"));
        assert!(expand(syn::parse_quote!(struct T(i32);))
            .unwrap_err()
            .to_string()
            .contains("named fields"));
    }
}
