//! Bulk-inserting rows with an `Appender`.
//!
//! `conn.appender(table, schema)` opens the fast bulk-ingest path: append each row with
//! `append`, then `save()` to flush the buffer (or let the appender flush on drop). Any
//! type implementing `AppendAble` is a row — here the built-in `DuckValue`.

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use common::{section, show, Result};

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (id INTEGER)")?;

    section("append a batch of rows, then save()");
    {
        let mut app = conn.appender("t", "main")?;
        for i in 0..1_000i32 {
            app.append(&mut DuckValue::Int(i))?;
        }
        // Flush the buffered rows so another read on the connection can see them.
        app.save()?;
        // `app` drops here (already saved): a bare, error-free teardown.
    }

    section("verify the rows landed");
    let count = conn.execute("SELECT count(*) AS n FROM t")?.next().expect("one row")?;
    show("row count", count.get("n"));
    assert_eq!(count.get("n"), Some(&DuckValue::BigInt(1000)));

    let sum = conn.execute("SELECT sum(id)::BIGINT AS s FROM t")?.next().expect("one row")?;
    show("sum(id)", sum.get("s"));
    // 0 + 1 + ... + 999 = 499500 (cast to BIGINT for a stable value type).
    assert_eq!(sum.get("s"), Some(&DuckValue::BigInt(499_500)));

    Ok(())
}
