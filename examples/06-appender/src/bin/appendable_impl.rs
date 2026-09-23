//! Implementing `AppendAble` for a custom row type.
//!
//! `AppendAble` is the trait that lets a value be appended by an [`Appender`] or bound to
//! a prepared statement. Its two methods hand you the raw DuckDB handle so you drive the
//! C API directly: `appender_append` writes the row's columns in order (inside the
//! appender's open row), and `stmt_append` binds them as consecutive parameters. Both are
//! `unsafe` at the FFI call sites, so each carries a SAFETY note.

use std::ffi::CString;

use better_duck_core::connection::Connection;
use better_duck_core::ffi::{
    duckdb_append_int32, duckdb_append_varchar, duckdb_appender, duckdb_bind_int32,
    duckdb_bind_varchar, duckdb_prepared_statement,
};
use better_duck_core::types::appendable::AppendAble;
use better_duck_core::types::value::DuckValue;
use common::{section, show, Result};

/// One row of a `(id INTEGER, name VARCHAR)` table.
struct Person {
    id: i32,
    name: String,
}

impl AppendAble for Person {
    fn appender_append(
        &mut self,
        appender: duckdb_appender,
    ) -> Result<()> {
        let c_name = CString::new(self.name.as_str())?;
        // SAFETY: `appender` is a valid duckdb_appender with a row open (the `Appender`
        // wrapper begins/ends the row around this call). We write exactly the table's two
        // columns in order — an INTEGER then a VARCHAR — and `c_name` is a valid
        // null-terminated string that outlives the call (DuckDB copies it).
        unsafe {
            duckdb_append_int32(appender, self.id);
            duckdb_append_varchar(appender, c_name.as_ptr());
        }
        Ok(())
    }

    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: duckdb_prepared_statement,
    ) -> Result<()> {
        let c_name = CString::new(self.name.as_str())?;
        // SAFETY: `stmt` is a valid prepared statement; `idx` is a 1-based parameter
        // index, so this row occupies `idx` (INTEGER) and `idx + 1` (VARCHAR). `c_name`
        // is a valid null-terminated string that outlives the call.
        unsafe {
            duckdb_bind_int32(stmt, idx, self.id);
            duckdb_bind_varchar(stmt, idx + 1, c_name.as_ptr());
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE people (id INTEGER, name VARCHAR)")?;

    section("append custom rows through the AppendAble impl");
    {
        let mut app = conn.appender("people", "main")?;
        app.append(&mut Person { id: 1, name: "Alice".to_owned() })?;
        app.append(&mut Person { id: 2, name: "Bao".to_owned() })?;
        app.append(&mut Person { id: 3, name: "Chen".to_owned() })?;
        app.save()?;
    }

    section("read the rows back");
    let rows: Vec<_> =
        conn.execute("SELECT id, name FROM people ORDER BY id")?.collect::<Result<_>>()?;
    for row in &rows {
        println!("  id={:?} name={:?}", row.get("id"), row.get("name"));
    }
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("id"), Some(&DuckValue::Int(1)));
    assert_eq!(rows[0].get("name"), Some(&DuckValue::Text("Alice".to_owned())));
    assert_eq!(rows[2].get("name"), Some(&DuckValue::Text("Chen".to_owned())));

    section("the same impl also binds as statement parameters");
    // `insert` uses `stmt_append`: one `Person` fills `$1` (id) and `$2` (name).
    conn.insert::<Person, _>(
        "INSERT INTO people VALUES ($1, $2)",
        std::iter::once(Person { id: 4, name: "Dara".to_owned() }),
    )?;
    let n = conn.execute("SELECT count(*) AS n FROM people")?.next().expect("row")?;
    show("row count after insert", n.get("n"));
    assert_eq!(n.get("n"), Some(&DuckValue::BigInt(4)));

    Ok(())
}
