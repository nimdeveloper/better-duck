//! SELECT / filter / order / paginate / aggregate with the Diesel DSL.
//!
//! Covers the predicate vocabulary (`eq` `ne` `gt` `lt` `ge` `le` `and` `or` `like` `between`
//! `is_null` `is_not_null` `eq_any` `ne_all`), ordering (`asc` / `desc`), `limit` / `offset`,
//! `distinct`, `count`, and the `first` / `load` result readers. A nullable `tag` column is
//! included so the NULL-aware predicates have something to work with.

use better_duck_diesel::DuckDbConnection;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;

diesel::table! {
    items (id) {
        id    -> Integer,
        label -> Text,
        score -> Integer,
        tag   -> Nullable<Text>,
    }
}

fn mem_conn() -> DuckDbConnection {
    let mut c = DuckDbConnection::establish(":memory:").expect("open in-memory DuckDB");
    c.batch_execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, label VARCHAR NOT NULL, score INTEGER NOT NULL, tag VARCHAR);
         INSERT INTO items VALUES
            (1,'alpha',   10, 'x'),
            (2,'beta',    20, 'y'),
            (3,'gamma',   30, 'x'),
            (4,'delta',   40, NULL),
            (5,'epsilon', 50, 'y');",
    )
    .expect("seed items");
    c
}

fn main() -> QueryResult<()> {
    let mut conn = mem_conn();

    println!("=== eq / ne ===");
    let eq_ids: Vec<i32> =
        items::table.filter(items::score.eq(30)).select(items::id).load(&mut conn)?;
    assert_eq!(eq_ids, [3]);
    let ne_count: i64 = items::table.filter(items::score.ne(30)).count().first(&mut conn)?;
    assert_eq!(ne_count, 4);
    println!("  eq(score=30) -> {eq_ids:?}; ne(score=30) count -> {ne_count}");

    println!("=== gt / lt / ge / le combined with and / or ===");
    let mid: Vec<i32> = items::table
        .filter(items::score.gt(10).and(items::score.lt(50)))
        .order(items::id)
        .select(items::id)
        .load(&mut conn)?;
    assert_eq!(mid, [2, 3, 4]);
    let edges: Vec<i32> = items::table
        .filter(items::score.le(10).or(items::score.ge(50)))
        .order(items::id)
        .select(items::id)
        .load(&mut conn)?;
    assert_eq!(edges, [1, 5]);
    println!("  10<score<50 -> {mid:?}; score<=10 OR score>=50 -> {edges:?}");

    println!("=== like ===");
    let like_ids: Vec<String> = items::table
        .filter(items::label.like("e%"))
        .order(items::id)
        .select(items::label)
        .load(&mut conn)?;
    assert_eq!(like_ids, ["epsilon"]);
    println!("  label LIKE 'e%' -> {like_ids:?}");

    println!("=== between ===");
    let between_ids: Vec<i32> = items::table
        .filter(items::score.between(20, 40))
        .order(items::id)
        .select(items::id)
        .load(&mut conn)?;
    assert_eq!(between_ids, [2, 3, 4]);
    println!("  score BETWEEN 20 AND 40 -> {between_ids:?}");

    println!("=== is_null / is_not_null ===");
    let null_ids: Vec<i32> =
        items::table.filter(items::tag.is_null()).select(items::id).load(&mut conn)?;
    assert_eq!(null_ids, [4]);
    let not_null: i64 = items::table.filter(items::tag.is_not_null()).count().first(&mut conn)?;
    assert_eq!(not_null, 4);
    println!("  tag IS NULL -> {null_ids:?}; tag IS NOT NULL count -> {not_null}");

    println!("=== eq_any / ne_all ===");
    let any_ids: Vec<i32> = items::table
        .filter(items::id.eq_any([1, 3, 5]))
        .order(items::id)
        .select(items::id)
        .load(&mut conn)?;
    assert_eq!(any_ids, [1, 3, 5]);
    let none_ids: Vec<i32> = items::table
        .filter(items::id.ne_all([1, 3, 5]))
        .order(items::id)
        .select(items::id)
        .load(&mut conn)?;
    assert_eq!(none_ids, [2, 4]);
    println!("  id = ANY(1,3,5) -> {any_ids:?}; id <> ALL(1,3,5) -> {none_ids:?}");

    println!("=== order asc / desc ===");
    let asc: Vec<i32> = items::table.order(items::score.asc()).select(items::id).load(&mut conn)?;
    assert_eq!(asc, [1, 2, 3, 4, 5]);
    let desc: Vec<i32> =
        items::table.order(items::score.desc()).select(items::id).load(&mut conn)?;
    assert_eq!(desc, [5, 4, 3, 2, 1]);
    println!("  by score asc -> {asc:?}; desc -> {desc:?}");

    println!("=== limit / offset ===");
    let page: Vec<i32> = items::table
        .order(items::id)
        .limit(2)
        .offset(1)
        .select(items::id)
        .load(&mut conn)?;
    assert_eq!(page, [2, 3]);
    println!("  limit 2 offset 1 -> {page:?}");

    println!("=== distinct ===");
    let mut tags: Vec<Option<String>> =
        items::table.select(items::tag).distinct().load(&mut conn)?;
    tags.sort();
    // NULL + "x" + "y" = 3 distinct tag values
    assert_eq!(tags.len(), 3);
    println!("  distinct tags -> {tags:?}");

    println!("=== count / first / load ===");
    let total: i64 = items::table.count().first(&mut conn)?;
    assert_eq!(total, 5);
    let first_label: String =
        items::table.order(items::id).select(items::label).first(&mut conn)?;
    assert_eq!(first_label, "alpha");
    let all_labels: Vec<String> =
        items::table.order(items::id).select(items::label).load(&mut conn)?;
    assert_eq!(all_labels.len(), 5);
    println!("  count -> {total}; first label -> {first_label:?}; loaded {} labels", all_labels.len());

    println!("\nAll select / filter forms verified.");
    Ok(())
}
