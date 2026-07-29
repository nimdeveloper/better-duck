//! Attribute-option parsing shared by `#[duckdb_scalar]` and
//! `#[duckdb_table_function]`.

use syn::{parse::Parser as _, punctuated::Punctuated, Expr, Lit, LitStr, Path, Token};

/// Options common to both macros: `name = "…"` and `crate = ::path`.
#[derive(Default)]
pub(crate) struct CommonAttrs {
    pub(crate) name: Option<LitStr>,
    pub(crate) crate_path: Option<Path>,
}

/// Parsed `#[duckdb_scalar(...)]` options.
#[derive(Default)]
pub(crate) struct ScalarAttrs {
    pub(crate) common: CommonAttrs,
    pub(crate) volatile: bool,
}

/// Parsed `#[duckdb_table_function(...)]` options.
#[derive(Default)]
pub(crate) struct TableAttrs {
    pub(crate) common: CommonAttrs,
    pub(crate) columns: Option<Vec<LitStr>>,
}

fn expect_str_lit(
    expr: &Expr,
    option: &str,
) -> syn::Result<LitStr> {
    match expr {
        Expr::Lit(lit) => match &lit.lit {
            Lit::Str(s) => Ok(s.clone()),
            other => {
                Err(syn::Error::new_spanned(other, format!("`{option}` must be a string literal")))
            },
        },
        other => {
            Err(syn::Error::new_spanned(other, format!("`{option}` must be a string literal")))
        },
    }
}

pub(crate) fn parse_scalar_attrs(attr: proc_macro::TokenStream) -> syn::Result<ScalarAttrs> {
    let mut out = ScalarAttrs::default();
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("name") {
            let value = meta.value()?;
            let lit = expect_str_lit(&value.parse::<Expr>()?, "name")?;
            if lit.value().is_empty() || lit.value().contains('\0') {
                return Err(syn::Error::new_spanned(
                    &lit,
                    "SQL function name must be non-empty and must not contain NUL bytes",
                ));
            }
            if out.common.name.is_some() {
                return Err(meta.error("`name` specified twice"));
            }
            out.common.name = Some(lit);
            Ok(())
        } else if meta.path.is_ident("crate") {
            let value = meta.value()?;
            out.common.crate_path = Some(value.parse::<Path>()?);
            Ok(())
        } else if meta.path.is_ident("volatile") {
            out.volatile = true;
            Ok(())
        } else {
            Err(meta.error("unknown option; expected one of `name`, `crate`, `volatile`"))
        }
    });
    parser.parse(attr)?;
    Ok(out)
}

pub(crate) fn parse_table_attrs(attr: proc_macro::TokenStream) -> syn::Result<TableAttrs> {
    let mut out = TableAttrs::default();
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("name") {
            let value = meta.value()?;
            let lit = expect_str_lit(&value.parse::<Expr>()?, "name")?;
            if lit.value().is_empty() || lit.value().contains('\0') {
                return Err(syn::Error::new_spanned(
                    &lit,
                    "SQL function name must be non-empty and must not contain NUL bytes",
                ));
            }
            if out.common.name.is_some() {
                return Err(meta.error("`name` specified twice"));
            }
            out.common.name = Some(lit);
            Ok(())
        } else if meta.path.is_ident("crate") {
            let value = meta.value()?;
            out.common.crate_path = Some(value.parse::<Path>()?);
            Ok(())
        } else if meta.path.is_ident("columns") {
            let content;
            syn::parenthesized!(content in meta.input);
            let lits = Punctuated::<LitStr, Token![,]>::parse_terminated(&content)?;
            out.columns = Some(lits.into_iter().collect());
            Ok(())
        } else if meta.path.is_ident("schema") {
            Err(meta.error(
                "`schema` is not supported — DuckDB's C API has no way to register a \
                 function directly into a schema; register it, then move it with SQL",
            ))
        } else {
            Err(meta.error("unknown option; expected one of `name`, `crate`, `columns`"))
        }
    });
    parser.parse(attr)?;
    Ok(out)
}
