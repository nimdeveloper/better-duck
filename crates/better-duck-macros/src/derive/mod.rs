//! Derive-macro code generation.

use syn::{DeriveInput, Field, LitStr, Path};

pub(crate) mod duck_enum;
pub(crate) mod duck_struct;
pub(crate) mod from_row;
pub(crate) mod to_row;

/// Container-level `#[duck(...)]` options shared by `#[derive(FromRow)]` and
/// `#[derive(DuckStruct)]`.
#[derive(Debug)]
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
#[derive(Clone, Copy, Debug)]
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

#[cfg(test)]
mod tests {
    use syn::{Data, DeriveInput, Fields};

    use super::{duck_field_name, parse_duck_container, RenameRule};

    fn first_field(di: &DeriveInput) -> &syn::Field {
        let Data::Struct(s) = &di.data else { panic!("not a struct") };
        let Fields::Named(n) = &s.fields else { panic!("not named") };
        n.named.first().unwrap()
    }

    #[test]
    fn rename_rule_applies_every_casing() {
        let apply = |r: &str, name: &str| RenameRule::from_name(r).unwrap().apply(name);
        assert_eq!(apply("lowercase", "userName"), "username");
        assert_eq!(apply("UPPERCASE", "userName"), "USERNAME");
        assert_eq!(apply("snake_case", "userName"), "user_name");
        assert_eq!(apply("SCREAMING_SNAKE_CASE", "userName"), "USER_NAME");
        assert_eq!(apply("kebab-case", "userName"), "user-name");
        assert_eq!(apply("SCREAMING-KEBAB-CASE", "userName"), "USER-NAME");
        assert_eq!(apply("camelCase", "user_name"), "userName");
        assert_eq!(apply("PascalCase", "user_name"), "UserName");
        // A run of uppercase letters is one word (no lower→Upper boundary inside it).
        assert_eq!(apply("snake_case", "HTTPServer2"), "httpserver2");
    }

    #[test]
    fn rename_rule_rejects_unknown() {
        assert!(RenameRule::from_name("Weird").unwrap_err().contains("unknown `rename_all`"));
    }

    #[test]
    fn container_reads_crate_and_rename_all() {
        let di: DeriveInput = syn::parse_quote! {
            #[duck(crate = ::my_core, rename_all = "SCREAMING_SNAKE_CASE")]
            struct S { field_one: i32 }
        };
        let c = parse_duck_container(&di).unwrap();
        assert!(matches!(c.rename_all, Some(RenameRule::ScreamingSnake)));
        let p = &c.crate_path;
        assert!(quote::quote!(#p).to_string().contains("my_core"));
    }

    #[test]
    fn container_rejects_unknown_and_bad_rename_all() {
        let bad: DeriveInput = syn::parse_quote! {
            #[duck(nope = 1)] struct S { a: i32 }
        };
        assert!(parse_duck_container(&bad).unwrap_err().to_string().contains("unknown `duck`"));
        let bad2: DeriveInput = syn::parse_quote! {
            #[duck(rename_all = "nonsense")] struct S { a: i32 }
        };
        assert!(parse_duck_container(&bad2)
            .unwrap_err()
            .to_string()
            .contains("unknown `rename_all`"));
    }

    #[test]
    fn field_name_prefers_rename_then_rename_all_then_ident() {
        let di: DeriveInput = syn::parse_quote! {
            struct S { #[duck(rename = "explicit")] a: i32 }
        };
        assert_eq!(duck_field_name(first_field(&di), None).unwrap(), "explicit");

        let di2: DeriveInput = syn::parse_quote! { struct S { some_field: i32 } };
        assert_eq!(
            duck_field_name(first_field(&di2), Some(RenameRule::Pascal)).unwrap(),
            "SomeField"
        );
        assert_eq!(duck_field_name(first_field(&di2), None).unwrap(), "some_field");
    }

    #[test]
    fn field_rejects_unknown_option() {
        let di: DeriveInput = syn::parse_quote! {
            struct S { #[duck(bogus = "x")] a: i32 }
        };
        assert!(duck_field_name(first_field(&di), None)
            .unwrap_err()
            .to_string()
            .contains("unknown `duck` field"));
    }
}
