//! Migration support: `MigrationConnection::setup` creates Diesel's bookkeeping table.
//!
//! Diesel tracks which migrations have run in a `__diesel_schema_migrations` table. Calling
//! `conn.setup()` (from the `MigrationConnection` trait) creates that table if it does not yet
//! exist. It is idempotent — safe to call on every startup.

use better_duck_diesel::DuckDbConnection;
use diesel::connection::SimpleConnection;
use diesel::migration::MigrationConnection;
use diesel::prelude::*;

fn main() -> QueryResult<()> {
    println!("=== setup() creates __diesel_schema_migrations ===");
    let mut conn = DuckDbConnection::establish(":memory:").expect("open in-memory DuckDB");
    conn.setup().expect("create migrations table");

    // The table now exists and can be queried (LIMIT 0 — we only check the shape).
    conn.batch_execute("SELECT version FROM __diesel_schema_migrations LIMIT 0")
        .expect("migrations table should exist");
    let versions_recorded: i64 =
        diesel::sql_query("SELECT count(*) AS count FROM __diesel_schema_migrations")
            .get_result::<CountRow>(&mut conn)?
            .count;
    println!("  migrations table created; recorded versions = {versions_recorded}");
    assert_eq!(versions_recorded, 0);

    println!("=== setup() is idempotent ===");
    // A second call must not error even though the table already exists.
    conn.setup().expect("second setup must be a no-op");
    println!("  second setup() call succeeded (no-op)");

    println!("\nMigration setup verified.");
    Ok(())
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}
