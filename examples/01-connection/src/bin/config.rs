//! Inspecting and building DuckDB configuration.
//!
//! `Config` is a consuming builder: each setter returns `Result<Config>`, so chain with
//! `?`. `library_version()` and the `flags()` iterator let you introspect the engine.

use better_duck_core::config::{
    library_version, AccessMode, Config, DefaultNullOrder, DefaultOrder,
};
use better_duck_core::connection::Connection;
use common::{section, show, Result};

fn main() -> Result<()> {
    section("library version");
    show("duckdb", library_version());

    section("available config flags (first 5)");
    show("flag_count", Config::flag_count());
    for flag in Config::flags().take(5) {
        println!("  - {}: {}", flag.name, flag.description);
    }

    section("build a fully-specified Config");
    let config = Config::default()
        .access_mode(AccessMode::ReadWrite)?
        .default_order(DefaultOrder::Desc)?
        .default_null_order(DefaultNullOrder::NullsLast)?
        .enable_external_access(true)?
        .enable_object_cache(true)?
        .max_memory("512MB")?
        .threads(2)?
        .custom_user_agent("better-duck-examples")?
        // `with` sets any arbitrary DuckDB setting by key/value.
        .with("default_null_order", "nulls_last")?;

    let mut conn = Connection::open_in_memory_with_flags(config)?;
    conn.execute_batch("CREATE TABLE t (v INTEGER)")?;
    assert!(conn.is_open());
    show("connection opened with custom config", conn.is_open());

    Ok(())
}
