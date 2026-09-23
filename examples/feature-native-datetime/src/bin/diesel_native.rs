//! Native (no-`chrono`) DATE / TIME / TIMESTAMP through the Diesel backend.
//!
//! With `chrono` off, the Diesel `Date` / `Time` / `Timestamp` SQL types bind and
//! read `better_duck_core`'s native structs (`DuckDate`, `DuckTime`) and
//! `std::time::SystemTime`. Values are written and read back with `sql_query` +
//! `QueryableByName`; a small `table!` schema is also exercised through the DSL.

use better_duck_core::error::Result;
use better_duck_core::types::date_native::{DuckDate, DuckTime};
use better_duck_diesel::DuckDbConnection;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sql_types::{Date, Time, Timestamp};
use std::time::{Duration, UNIX_EPOCH};

diesel::table! {
    events (id) {
        id -> Integer,
        d  -> Date,
        t  -> Time,
        ts -> Timestamp,
    }
}

/// One row read back by name, decoded into the native temporal types.
#[derive(diesel::QueryableByName, Debug)]
struct EventRow {
    #[diesel(sql_type = Date)]
    d: DuckDate,
    #[diesel(sql_type = Time)]
    t: DuckTime,
    #[diesel(sql_type = Timestamp)]
    ts: std::time::SystemTime,
}

fn main() -> Result<()> {
    let mut conn = DuckDbConnection::establish(":memory:").expect("open in-memory DuckDB");
    conn.batch_execute("CREATE TABLE events (id INTEGER, d DATE, t TIME, ts TIMESTAMP)")
        .expect("create table");

    let d = DuckDate { year: 2024, month: 6, day: 15 };
    let t = DuckTime { hour: 14, min: 30, sec: 45, micros: 123_456 };
    let ts = UNIX_EPOCH + Duration::from_secs(1_717_243_200) + Duration::from_micros(654_321);

    // Write via sql_query bind parameters (ToSql for the native types).
    diesel::sql_query("INSERT INTO events VALUES (1, $1, $2, $3)")
        .bind::<Date, _>(d)
        .bind::<Time, _>(t)
        .bind::<Timestamp, _>(ts)
        .execute(&mut conn)
        .expect("insert row");

    // Read via sql_query + QueryableByName (FromSql for the native types).
    let row: EventRow = diesel::sql_query("SELECT d, t, ts FROM events WHERE id = 1")
        .get_result(&mut conn)
        .expect("read row by name");
    println!("date  -> {:?}", row.d);
    println!("time  -> {:?}", row.t);
    println!("stamp -> {:?}", row.ts);
    assert_eq!(row.d, d);
    assert_eq!(row.t, t);
    assert_eq!(row.ts, ts);

    // The `events` table! documents the schema. Its fully-`Queryable` `id` column is
    // read through the DSL; the native Date/Time/Timestamp columns are read above via
    // the sql_query + QueryableByName (FromSql) path, which is what the native temporal
    // types implement.
    let ids: Vec<i32> =
        events::table.order(events::id).select(events::id).load(&mut conn).expect("load ids");
    assert_eq!(ids, vec![1]);
    // Name the temporal columns so the full schema is referenced.
    let _schema = (events::d, events::t, events::ts);

    println!("\nDiesel Date/Time/Timestamp round-tripped through native types");
    Ok(())
}
