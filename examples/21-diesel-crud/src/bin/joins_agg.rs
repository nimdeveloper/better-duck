//! Joins, aggregates, and boxed pagination.
//!
//! Two tables related by a foreign key: `diesel::joinable!` teaches Diesel the default join
//! condition and `allow_tables_to_appear_in_same_query!` lets them share a query.
//! `inner_join` keeps only matched rows; `left_join` keeps every left row and yields
//! `.nullable()` columns for the right side. `diesel::dsl::{sum, avg, min, max}` and
//! `.count()` compute aggregates; `into_boxed()` erases the query type for runtime pagination.

use better_duck_diesel::DuckDbConnection;
use diesel::connection::SimpleConnection;
use diesel::dsl::{avg, max, min, sum};
use diesel::prelude::*;

diesel::table! {
    owners (id) {
        id   -> Integer,
        name -> Text,
    }
}

diesel::table! {
    items (id) {
        id       -> Integer,
        label    -> Text,
        score    -> Integer,
        price    -> Double,
        owner_id -> Nullable<Integer>,
    }
}

diesel::joinable!(items -> owners (owner_id));
diesel::allow_tables_to_appear_in_same_query!(items, owners);

fn mem_conn() -> DuckDbConnection {
    let mut c = DuckDbConnection::establish(":memory:").expect("open in-memory DuckDB");
    c.batch_execute(
        "CREATE TABLE owners (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL);
         CREATE TABLE items (
            id INTEGER PRIMARY KEY, label VARCHAR NOT NULL,
            score INTEGER NOT NULL, price DOUBLE NOT NULL, owner_id INTEGER
         );
         INSERT INTO owners VALUES (1,'Ada'),(2,'Grace');
         INSERT INTO items VALUES
            (1,'alpha',   10, 1.50, 1),
            (2,'beta',    20, 2.50, 1),
            (3,'gamma',   30, 3.00, 2),
            (4,'orphan',  40, 4.00, NULL);",
    )
    .expect("seed data");
    c
}

fn main() -> QueryResult<()> {
    let mut conn = mem_conn();

    println!("=== inner_join (only matched rows) ===");
    let matched: Vec<(String, String)> = items::table
        .inner_join(owners::table)
        .select((items::label, owners::name))
        .order(items::id)
        .load(&mut conn)?;
    // The orphan item (owner_id NULL) drops out of the inner join.
    assert_eq!(
        matched,
        [
            ("alpha".to_owned(), "Ada".to_owned()),
            ("beta".to_owned(), "Ada".to_owned()),
            ("gamma".to_owned(), "Grace".to_owned()),
        ]
    );
    println!("  {} matched rows", matched.len());

    println!("=== left_join with .on(...) preserving unmatched rows ===");
    let all: Vec<(String, Option<String>)> = items::table
        .left_join(owners::table.on(items::owner_id.eq(owners::id.nullable())))
        .select((items::label, owners::name.nullable()))
        .order(items::id)
        .load(&mut conn)?;
    assert_eq!(all.len(), 4);
    assert_eq!(all[0], ("alpha".to_owned(), Some("Ada".to_owned())));
    assert_eq!(all[3], ("orphan".to_owned(), None)); // preserved, owner is NULL
    println!("  {} rows (orphan kept with NULL owner)", all.len());

    println!("=== aggregates: sum / avg / min / max / count ===");
    // DuckDB widens SUM/AVG of a floating column to DOUBLE (-> f64). (SUM over an INTEGER
    // column widens to HUGEINT, which the BigInt adapter would reject — so aggregate the
    // DOUBLE `price` column here.)
    let total: Option<f64> = items::table.select(sum(items::price)).first(&mut conn)?;
    assert!((total.unwrap() - 11.0).abs() < 1e-9);
    let average: Option<f64> = items::table.select(avg(items::price)).first(&mut conn)?;
    assert!((average.unwrap() - 2.75).abs() < 1e-9);
    let lowest: Option<i32> = items::table.select(min(items::score)).first(&mut conn)?;
    let highest: Option<i32> = items::table.select(max(items::score)).first(&mut conn)?;
    assert_eq!((lowest, highest), (Some(10), Some(40)));
    let n: i64 = items::table.count().first(&mut conn)?;
    assert_eq!(n, 4);
    println!("  sum(price)={total:?} avg(price)={average:?} min(score)={lowest:?} max(score)={highest:?} count={n}");

    println!("=== into_boxed() pagination ===");
    let page = |offset: i64, limit: i64, conn: &mut DuckDbConnection| -> QueryResult<Vec<i32>> {
        // A boxed query can accumulate limit/offset decided at runtime.
        items::table
            .into_boxed()
            .order(items::id)
            .offset(offset)
            .limit(limit)
            .select(items::id)
            .load(conn)
    };
    let page1 = page(0, 2, &mut conn)?;
    let page2 = page(2, 2, &mut conn)?;
    assert_eq!(page1, [1, 2]);
    assert_eq!(page2, [3, 4]);
    println!("  page1 -> {page1:?}; page2 -> {page2:?}");

    println!("\nJoins, aggregates, and pagination verified.");
    Ok(())
}
