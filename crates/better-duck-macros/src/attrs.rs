//! Attribute-option parsing shared by `#[duckdb_scalar]` and
//! `#[duckdb_table_function]`.

use syn::{parse::Parser as _, punctuated::Punctuated, Expr, Lit, LitStr, Path, Token, Type};

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
    /// `state(Type, init_expr)` — the function's shared `VScalar::State` type
    /// and its initial value, readable inside the fn body via `duck_state!`.
    pub(crate) state: Option<(Type, Expr)>,
}

/// Parsed `#[duckdb_table_function(...)]` options.
#[derive(Default)]
pub(crate) struct TableAttrs {
    pub(crate) common: CommonAttrs,
    pub(crate) columns: Option<Vec<LitStr>>,
    /// Names of the fn's parameters to bind as SQL named (keyword) parameters
    /// instead of positional ones — each must match a parameter identifier.
    pub(crate) named_params: Option<Vec<LitStr>>,
    /// Whether the generated function honors `duck_projection!()` — see
    /// `VTab::supports_projection_pushdown`.
    pub(crate) projection_pushdown: bool,
    /// `extra_info(Type, init_expr)` — a value shared read-only across every
    /// call, readable inside the fn body via `duck_extra_info!(Type)`.
    pub(crate) extra_info: Option<(Type, Expr)>,
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
        } else if meta.path.is_ident("state") {
            if out.state.is_some() {
                return Err(meta.error("`state` specified twice"));
            }
            let content;
            syn::parenthesized!(content in meta.input);
            let ty: Type = content.parse()?;
            content.parse::<Token![,]>()?;
            let expr: Expr = content.parse()?;
            out.state = Some((ty, expr));
            Ok(())
        } else {
            Err(meta.error("unknown option; expected one of `name`, `crate`, `volatile`, `state`"))
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
        } else if meta.path.is_ident("named_params") {
            let content;
            syn::parenthesized!(content in meta.input);
            let lits = Punctuated::<LitStr, Token![,]>::parse_terminated(&content)?;
            if lits.is_empty() {
                return Err(meta.error("`named_params` must list at least one parameter name"));
            }
            out.named_params = Some(lits.into_iter().collect());
            Ok(())
        } else if meta.path.is_ident("projection_pushdown") {
            out.projection_pushdown = true;
            Ok(())
        } else if meta.path.is_ident("extra_info") {
            if out.extra_info.is_some() {
                return Err(meta.error("`extra_info` specified twice"));
            }
            let content;
            syn::parenthesized!(content in meta.input);
            let ty: Type = content.parse()?;
            content.parse::<Token![,]>()?;
            let expr: Expr = content.parse()?;
            out.extra_info = Some((ty, expr));
            Ok(())
        } else if meta.path.is_ident("schema") {
            Err(meta.error(
                "`schema` is not supported — DuckDB's C API has no way to register a \
                 function directly into a schema; register it, then move it with SQL",
            ))
        } else {
            Err(meta.error(
                "unknown option; expected one of `name`, `crate`, `columns`, `named_params`, \
                 `projection_pushdown`, `extra_info`",
            ))
        }
    });
    parser.parse(attr)?;
    Ok(out)
}
