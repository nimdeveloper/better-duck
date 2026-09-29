#![allow(missing_docs)]
//! The ICU extension is bundled into the static library (feature `icu`), so ICU-only
//! SQL — named IANA timezones, Unicode collations — works with **no runtime
//! `INSTALL`/`LOAD`**. These queries fail on a core-only build ("Unknown TimeZone" /
//! unknown collation), so their success proves ICU is linked in.
//!
//! Gated at the item level (not the crate) so the test binary still compiles cleanly
//! when the `icu` feature is off — it just contains no tests.

#[cfg(feature = "icu")]
use better_duck_core::{connection::Connection, types::value::DuckValue};

#[cfg(feature = "icu")]
#[test]
fn named_timezone_available_without_runtime_load() {
    let mut conn = Connection::open_in_memory().unwrap();
    // Named IANA zones (as opposed to fixed offsets) are provided by ICU.
    let mut rows = conn
        .execute(
            "SELECT (TIMESTAMPTZ '2024-06-01 12:00:00+00' AT TIME ZONE 'America/New_York')::VARCHAR AS local",
        )
        .expect("named timezone should resolve with icu bundled");
    let row = rows.next().unwrap().unwrap();
    match row.get("local") {
        // 12:00 UTC in June is 08:00 in New York (EDT, UTC-4).
        Some(DuckValue::Text(s)) => assert!(s.contains("08:00"), "expected 08:00 EDT, got {s}"),
        other => panic!("unexpected value for `local`: {other:?}"),
    }
}

#[cfg(feature = "icu")]
#[test]
fn icu_collation_available_without_runtime_load() {
    let mut conn = Connection::open_in_memory().unwrap();
    // ICU registers language collations like `de`; ordering under it is a collation the
    // core-only build doesn't know.
    let mut rows = conn
        .execute("SELECT ('a' COLLATE de) < ('b' COLLATE de) AS ordered")
        .expect("icu collation should be available with icu bundled");
    let row = rows.next().unwrap().unwrap();
    assert_eq!(row.get("ordered"), Some(&DuckValue::Boolean(true)));
}
