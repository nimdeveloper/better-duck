//! Attribute-option parsing shared by `#[duckdb_scalar]` and
//! `#[duckdb_table_function]`.

use syn::{parse::Parser as _, punctuated::Punctuated, Expr, Lit, LitStr, Path, Token, Type};

/// Options common to both macros: `name = "…"` and `crate = ::path`.
#[derive(Default, Debug)]
pub(crate) struct CommonAttrs {
    pub(crate) name: Option<LitStr>,
    pub(crate) crate_path: Option<Path>,
}

/// Parsed `#[duckdb_scalar(...)]` options.
#[derive(Default, Debug)]
pub(crate) struct ScalarAttrs {
    pub(crate) common: CommonAttrs,
    pub(crate) volatile: bool,
    /// `state(Type, init_expr)` — the function's shared `VScalar::State` type
    /// and its initial value, readable inside the fn body via `duck_state!`.
    pub(crate) state: Option<(Type, Expr)>,
}

/// Parsed `#[duckdb_table_function(...)]` options.
#[derive(Default, Debug)]
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

/// Parsed `#[duckdb_cast(...)]` options. Casts are unnamed, so there is no `name`.
#[derive(Default, Debug)]
pub(crate) struct CastAttrs {
    /// `crate = ::path` escape hatch for the generated `::better_duck_core` path.
    pub(crate) crate_path: Option<Path>,
    /// `implicit_cost = <int>` — the binder's implicit-cast cost (lower = preferred;
    /// negative = explicit-only). Defaults to `-1` (explicit `CAST` only).
    pub(crate) implicit_cost: Option<i64>,
}

/// Parsed `#[duckdb_aggregate(...)]` options.
#[derive(Default, Debug)]
pub(crate) struct AggregateAttrs {
    pub(crate) common: CommonAttrs,
    /// Whether the aggregate should still be invoked for `NULL` inputs.
    pub(crate) special_handling: bool,
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

pub(crate) fn parse_scalar_attrs(attr: proc_macro2::TokenStream) -> syn::Result<ScalarAttrs> {
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
    parser.parse2(attr)?;
    Ok(out)
}

pub(crate) fn parse_table_attrs(attr: proc_macro2::TokenStream) -> syn::Result<TableAttrs> {
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
    parser.parse2(attr)?;
    Ok(out)
}

/// Parses a signed integer literal (`N` or `-N`) for an integer-valued option.
fn expect_i64(
    expr: &Expr,
    option: &str,
) -> syn::Result<i64> {
    match expr {
        Expr::Lit(lit) => match &lit.lit {
            Lit::Int(i) => i.base10_parse::<i64>(),
            other => Err(syn::Error::new_spanned(other, format!("`{option}` must be an integer"))),
        },
        Expr::Unary(u) if matches!(u.op, syn::UnOp::Neg(_)) => {
            if let Expr::Lit(lit) = &*u.expr {
                if let Lit::Int(i) = &lit.lit {
                    return Ok(-i.base10_parse::<i64>()?);
                }
            }
            Err(syn::Error::new_spanned(expr, format!("`{option}` must be an integer")))
        },
        other => Err(syn::Error::new_spanned(other, format!("`{option}` must be an integer"))),
    }
}

pub(crate) fn parse_cast_attrs(attr: proc_macro2::TokenStream) -> syn::Result<CastAttrs> {
    let mut out = CastAttrs::default();
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("crate") {
            out.crate_path = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("implicit_cost") {
            let expr: Expr = meta.value()?.parse()?;
            out.implicit_cost = Some(expect_i64(&expr, "implicit_cost")?);
            Ok(())
        } else {
            Err(meta.error("unknown option; expected one of `crate`, `implicit_cost`"))
        }
    });
    parser.parse2(attr)?;
    Ok(out)
}

pub(crate) fn parse_aggregate_attrs(attr: proc_macro2::TokenStream) -> syn::Result<AggregateAttrs> {
    let mut out = AggregateAttrs::default();
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
        } else if meta.path.is_ident("special_handling") {
            out.special_handling = true;
            Ok(())
        } else {
            Err(meta.error("unknown option; expected one of `name`, `crate`, `special_handling`"))
        }
    });
    parser.parse2(attr)?;
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

    use super::{
        expect_i64, parse_aggregate_attrs, parse_cast_attrs, parse_scalar_attrs, parse_table_attrs,
    };

    #[test]
    fn scalar_attrs_parse_every_option() {
        let a =
            parse_scalar_attrs(quote!(name = "f", crate = ::foo, volatile, state(i64, 0))).unwrap();
        assert_eq!(a.common.name.unwrap().value(), "f");
        assert!(a.common.crate_path.is_some());
        assert!(a.volatile);
        assert!(a.state.is_some());
        // Empty attr → all defaults.
        let d = parse_scalar_attrs(quote!()).unwrap();
        assert!(d.common.name.is_none() && !d.volatile && d.state.is_none());
    }

    #[test]
    fn scalar_attrs_reject_bad_input() {
        assert!(parse_scalar_attrs(quote!(name = ""))
            .unwrap_err()
            .to_string()
            .contains("non-empty"));
        assert!(parse_scalar_attrs(quote!(name = "a", name = "b"))
            .unwrap_err()
            .to_string()
            .contains("twice"));
        assert!(parse_scalar_attrs(quote!(state(i64, 0), state(i32, 1)))
            .unwrap_err()
            .to_string()
            .contains("twice"));
        assert!(parse_scalar_attrs(quote!(bogus))
            .unwrap_err()
            .to_string()
            .contains("unknown option"));
        assert!(parse_scalar_attrs(quote!(name = 42))
            .unwrap_err()
            .to_string()
            .contains("string literal"));
    }

    #[test]
    fn table_attrs_parse_and_reject() {
        let a = parse_table_attrs(quote!(
            name = "t",
            columns("a", "b"),
            named_params("p"),
            projection_pushdown,
            extra_info(u8, 0)
        ))
        .unwrap();
        assert_eq!(a.columns.unwrap().len(), 2);
        assert_eq!(a.named_params.unwrap().len(), 1);
        assert!(a.projection_pushdown && a.extra_info.is_some());
        assert!(parse_table_attrs(quote!(named_params()))
            .unwrap_err()
            .to_string()
            .contains("at least one"));
        assert!(parse_table_attrs(quote!(schema = "s"))
            .unwrap_err()
            .to_string()
            .contains("not supported"));
        assert!(parse_table_attrs(quote!(nope))
            .unwrap_err()
            .to_string()
            .contains("unknown option"));
    }

    #[test]
    fn cast_and_aggregate_attrs() {
        let c = parse_cast_attrs(quote!(crate = ::x, implicit_cost = -5)).unwrap();
        assert_eq!(c.implicit_cost, Some(-5));
        assert!(c.crate_path.is_some());
        assert_eq!(parse_cast_attrs(quote!(implicit_cost = 7)).unwrap().implicit_cost, Some(7));
        assert!(parse_cast_attrs(quote!(implicit_cost = "x"))
            .unwrap_err()
            .to_string()
            .contains("must be an integer"));
        assert!(parse_cast_attrs(quote!(bad)).unwrap_err().to_string().contains("unknown option"));

        let g = parse_aggregate_attrs(quote!(name = "agg", special_handling)).unwrap();
        assert_eq!(g.common.name.unwrap().value(), "agg");
        assert!(g.special_handling);
        assert!(parse_aggregate_attrs(quote!(what))
            .unwrap_err()
            .to_string()
            .contains("unknown option"));
    }

    #[test]
    fn expect_i64_handles_sign_and_errors() {
        assert_eq!(expect_i64(&parse_quote!(3), "c").unwrap(), 3);
        assert_eq!(expect_i64(&parse_quote!(-3), "c").unwrap(), -3);
        assert!(expect_i64(&parse_quote!("x"), "c").unwrap_err().to_string().contains("integer"));
        assert!(expect_i64(&parse_quote!(-"x"), "c").unwrap_err().to_string().contains("integer"));
    }
}
