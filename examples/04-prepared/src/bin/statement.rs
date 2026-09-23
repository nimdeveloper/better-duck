//! Prepared statements via `conn.db().prepare(...)` — bind, execute, reuse.
//!
//! There is no `Connection::prepare`; the entry point is `conn.db().prepare(sql)`, which
//! returns a `Statement` borrowing the connection. `bind` auto-increments the positional
//! index; `bind_at` targets a specific 1-based index; `clear_bindings` resets for reuse.

use better_duck_core::connection::Connection;
use common::{section, show, Result};

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch(
        "CREATE TABLE nums (v INTEGER);
         INSERT INTO nums VALUES (10), (20), (30), (40);",
    )?;

    section("prepare once, bind sequentially, execute");
    let mut stmt = conn.db().prepare("SELECT v FROM nums WHERE v >= $1 AND v <= $2")?;
    show("bind_parameter_count", stmt.bind_parameter_count());

    // `bind` fills $1 then $2 in call order.
    stmt.bind(&mut 10i32)?;
    stmt.bind(&mut 20i32)?;
    let n = stmt.execute()?.count();
    show("rows in [10, 20]", n);
    assert_eq!(n, 2);

    section("reuse: clear_bindings + bind_at (explicit 1-based index)");
    stmt.clear_bindings()?;
    stmt.bind_at(&mut 20i32, 1)?;
    stmt.bind_at(&mut 40i32, 2)?;
    let n = stmt.execute()?.count();
    show("rows in [20, 40]", n);
    assert_eq!(n, 3);

    Ok(())
}
