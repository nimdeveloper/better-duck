//! Replacement scans the low-level way: implement `ReplacementScan` and register
//! it on a `Database` with `db.register_replacement_scan::<T>()`.
//!
//! A replacement scan rewrites an unresolved table reference into a table
//! function call — e.g. routing `SELECT * FROM '5.range'` to `range(5)` — without
//! the caller writing the function call explicitly. Registration is per-database
//! (it applies to every connection), which is why it lives on `Database`, not
//! `Connection`.

use better_duck_core::types::value::DuckValue;
use better_duck_core::udf::{ReplacementScan, ReplacementScanInfo, UdfResult};
use better_duck_core::Database;
use common::{section, show, Result};

/// Routes any unresolved name ending in `.range` to `range(n)`, where `n` is the
/// integer stem before the extension — so `'5.range'` becomes `range(5)`.
struct RangeByExtension;

impl ReplacementScan for RangeByExtension {
    fn replace(
        table_name: &str,
        info: &ReplacementScanInfo,
    ) -> UdfResult<()> {
        let Some(stem) = table_name.strip_suffix(".range") else {
            return Ok(()); // Decline: not our extension, leave DuckDB's normal error.
        };
        let n: i64 = stem.parse().map_err(|_| format!("'{table_name}' has a non-integer stem"))?;
        info.set_function_name("range")?;
        info.add_parameter(&n)?;
        Ok(())
    }
}

fn main() -> Result<()> {
    let db = Database::open_in_memory()?;
    db.register_replacement_scan::<RangeByExtension>();
    let mut conn = db.connect()?;

    section("SELECT * FROM '5.range' is rewritten to range(5)");
    let result = conn.execute("SELECT * FROM '5.range' ORDER BY range")?;
    let rows: Vec<_> = result.collect::<Result<_>>()?;
    let got: Vec<&DuckValue> = rows.iter().filter_map(|row| row.get("range")).collect();
    show("'5.range'", &got);
    assert_eq!(rows.len(), 5);
    assert_eq!(got[0], &DuckValue::BigInt(0));
    assert_eq!(got[4], &DuckValue::BigInt(4));

    section("declining leaves DuckDB's normal 'table not found' error");
    let err = conn.execute("SELECT * FROM does_not_exist").err().expect("an error");
    show("error", err.to_string());
    assert!(err.to_string().to_lowercase().contains("does_not_exist"), "{err}");

    section("a bail from replace surfaces as a query error, connection stays usable");
    let err = conn.execute("SELECT * FROM 'not-a-number.range'").err().expect("an error");
    show("error", err.to_string());
    assert!(err.to_string().contains("non-integer stem"), "{err}");
    conn.execute_batch("CREATE TABLE t (v INTEGER)")?;
    show("recovered", true);

    Ok(())
}
