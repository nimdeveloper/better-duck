//! Derive-macro code generation.

use syn::{DeriveInput, Field, LitStr, Path};

pub(crate) mod duck_enum;
pub(crate) mod duck_struct;
pub(crate) mod from_row;
pub(crate) mod to_row;

/// Container-level `#[duck(...)]` options shared by `#[derive(FromRow)]` and
/// `#[derive(DuckStruct)]`.
pub(crate) struct DuckContainer {
    pub(crate) crate_path: Path,
    pub(crate) rename_all: Option<RenameRule>,
}

/// Parses the container `#[duck(crate = …, rename_all = "…")]` options.
pub(crate) fn parse_duck_container(input: &DeriveInput) -> syn::Result<DuckContainer> {
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
    Ok(DuckContainer { crate_path, rename_all })
}

/// Reads a field's `#[duck(rename = "...")]`, if present.
pub(crate) fn duck_field_rename(field: &Field) -> syn::Result<Option<String>> {
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

/// Resolves a field's DuckDB column/field name from its `rename`, the container
/// `rename_all`, or the field identifier.
pub(crate) fn duck_field_name(
    field: &Field,
    rename_all: Option<RenameRule>,
) -> syn::Result<String> {
    let ident = field.ident.as_ref().expect("named field has an ident").to_string();
    Ok(match duck_field_rename(field)? {
        Some(name) => name,
        None => rename_all.map_or(ident.clone(), |r| r.apply(&ident)),
    })
}

/// A `rename_all` casing rule, applied to a Rust field or variant name.
///
/// Names are split into words on `_`/`-` and at `lower→Upper` case boundaries, so
/// both `snake_case` fields and `PascalCase`/`camelCase` variants map correctly.
#[derive(Clone, Copy)]
pub(crate) enum RenameRule {
    Lower,
    Upper,
    Snake,
    ScreamingSnake,
    Kebab,
    ScreamingKebab,
    Camel,
    Pascal,
}

/// Splits `s` into words on `_`/`-` separators and `lower→Upper` boundaries.
fn split_words(s: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut cur = String::new();
    let mut prev_is_lower = false;
    for ch in s.chars() {
        if ch == '_' || ch == '-' {
            if !cur.is_empty() {
                words.push(std::mem::take(&mut cur));
            }
            prev_is_lower = false;
        } else if ch.is_uppercase() && prev_is_lower {
            if !cur.is_empty() {
                words.push(std::mem::take(&mut cur));
            }
            cur.push(ch);
            prev_is_lower = false;
        } else {
            prev_is_lower = ch.is_lowercase() || ch.is_ascii_digit();
            cur.push(ch);
        }
    }
    if !cur.is_empty() {
        words.push(cur);
    }
    words
}

/// Lowercases `w`, then upper-cases its first character.
fn capitalize(w: &str) -> String {
    let lower = w.to_lowercase();
    let mut chars = lower.chars();
    chars.next().map_or_else(String::new, |f| f.to_uppercase().chain(chars).collect())
}

impl RenameRule {
    pub(crate) fn from_name(s: &str) -> Result<RenameRule, &'static str> {
        Ok(match s {
            "lowercase" => RenameRule::Lower,
            "UPPERCASE" => RenameRule::Upper,
            "snake_case" => RenameRule::Snake,
            "SCREAMING_SNAKE_CASE" => RenameRule::ScreamingSnake,
            "kebab-case" => RenameRule::Kebab,
            "SCREAMING-KEBAB-CASE" => RenameRule::ScreamingKebab,
            "camelCase" => RenameRule::Camel,
            "PascalCase" => RenameRule::Pascal,
            _ => {
                return Err("unknown `rename_all`; expected one of \"lowercase\", \"UPPERCASE\", \
                            \"snake_case\", \"SCREAMING_SNAKE_CASE\", \"kebab-case\", \
                            \"SCREAMING-KEBAB-CASE\", \"camelCase\", \"PascalCase\"")
            },
        })
    }

    /// Applies the rule to a field or variant name.
    pub(crate) fn apply(
        self,
        name: &str,
    ) -> String {
        let words = split_words(name);
        let lower = |w: &String| w.to_lowercase();
        match self {
            RenameRule::Lower => words.iter().map(lower).collect(),
            RenameRule::Upper => words.iter().map(|w| w.to_uppercase()).collect(),
            RenameRule::Snake => words.iter().map(lower).collect::<Vec<_>>().join("_"),
            RenameRule::ScreamingSnake => {
                words.iter().map(|w| w.to_uppercase()).collect::<Vec<_>>().join("_")
            },
            RenameRule::Kebab => words.iter().map(lower).collect::<Vec<_>>().join("-"),
            RenameRule::ScreamingKebab => {
                words.iter().map(|w| w.to_uppercase()).collect::<Vec<_>>().join("-")
            },
            RenameRule::Pascal => words.iter().map(|w| capitalize(w)).collect(),
            RenameRule::Camel => {
                let mut out = String::new();
                for (i, w) in words.iter().enumerate() {
                    if i == 0 {
                        out.push_str(&w.to_lowercase());
                    } else {
                        out.push_str(&capitalize(w));
                    }
                }
                out
            },
        }
    }
}
