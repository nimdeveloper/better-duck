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
    /// Output column names; the types are inferred from the iterator item tuple.
    pub(crate) columns: Option<Vec<LitStr>>,
    /// Names of parameters that should be registered as DuckDB named parameters
    /// instead of positional parameters.
    pub(crate) named_params: Option<Vec<LitStr>>,
    /// Whether DuckDB may push a column projection into the function.
    pub(crate) projection_pushdown: bool,
    /// `extra_info(Type, expr)` — arbitrary shared data attached to the bind
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
            let expr: Expr = meta.value()?.parse()?;
            let lit = expect_str_lit(&expr, "name")?;
            if lit.value().is_empty() || lit.value().contains('\0') {
                return Err(syn::Error::new_spanned(
                    lit,
                    "`name` must be a non-empty string without NUL bytes",
                ));
            }
            if out.common.name.is_some() {
                return Err(meta.error("`name` specified twice"));
            }
            out.common.name = Some(lit);
            Ok(())
        } else if meta.path.is_ident("crate") {
            out.common.crate_path = Some(meta.value()?.parse()?);
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
            let expr: Expr = meta.value()?.parse()?;
            let lit = expect_str_lit(&expr, "name")?;
            if lit.value().is_empty() || lit.value().contains('\0') {
                return Err(syn::Error::new_spanned(
                    lit,
                    "`name` must be a non-empty string without NUL bytes",
                ));
            }
            if out.common.name.is_some() {
                return Err(meta.error("`name` specified twice"));
            }
            out.common.name = Some(lit);
            Ok(())
        } else if meta.path.is_ident("crate") {
            out.common.crate_path = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("columns") {
            let content;
            syn::parenthesized!(content in meta.input);
            let exprs = Punctuated::<Expr, Token![,]>::parse_terminated(&content)?;
            out.columns = Some(
                exprs.iter().map(|e| expect_str_lit(e, "columns")).collect::<syn::Result<_>>()?,
            );
            Ok(())
        } else if meta.path.is_ident("named_params") {
            let content;
            syn::parenthesized!(content in meta.input);
            let exprs = Punctuated::<Expr, Token![,]>::parse_terminated(&content)?;
            let names: Vec<LitStr> = exprs
                .iter()
                .map(|e| expect_str_lit(e, "named_params"))
                .collect::<syn::Result<_>>()?;
            if names.is_empty() {
                return Err(meta.error("`named_params` must list at least one parameter name"));
            }
            out.named_params = Some(names);
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

#[cfg(test)]
mod tests {
    use quote::quote;
    use syn::parse_quote;

    use super::expect_str_lit;

    #[test]
    fn expect_str_lit_accepts_a_string() {
        let expr = parse_quote!("duck");
        assert_eq!(expect_str_lit(&expr, "name").unwrap().value(), "duck");
    }

    #[test]
    fn expect_str_lit_rejects_other_literals() {
        let expr = parse_quote!(42);
        let error = expect_str_lit(&expr, "columns").unwrap_err();
        assert!(error.to_string().contains("`columns` must be a string literal"));
    }

    #[test]
    fn expect_str_lit_rejects_non_literal_expressions() {
        let expr = parse_quote!(COLUMN_NAME);
        let error = expect_str_lit(&expr, "named_params").unwrap_err();
        assert!(error.to_string().contains("`named_params` must be a string literal"));
    }

    #[test]
    fn string_literal_round_trips_tokens() {
        let expr = parse_quote!("a column");
        let literal = expect_str_lit(&expr, "columns").unwrap();
        assert_eq!(quote!(#literal).to_string(), "\"a column\"");
    }
}
