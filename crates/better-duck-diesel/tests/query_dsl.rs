#![allow(missing_docs)]
//! Query DSL tests: filter predicates, ordering, pagination, aggregates, joins.

use better_duck_diesel::DuckDbConnection;
use diesel::{connection::SimpleConnection, prelude::*};

// Schema

diesel::table! {
    products (id) {
        id       -> Integer,
        name     -> Text,
        price    -> Double,
        category -> Nullable<Text>,
    }
}

diesel::table! {
    categories (id) {
        id   -> Integer,
        name -> Text,
    }
}

diesel::joinable!(products -> categories (id));
diesel::allow_tables_to_appear_in_same_query!(products, categories);

fn mem_conn() -> DuckDbConnection {
    let mut c = DuckDbConnection::establish(":memory:").expect("open in-memory DuckDB");
    c.batch_execute(
        "CREATE TABLE categories (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL);
         CREATE TABLE products (
             id INTEGER PRIMARY KEY,
             name VARCHAR NOT NULL,
             price DOUBLE NOT NULL,
             category VARCHAR
         );
         INSERT INTO categories VALUES (1,'Electronics'),(2,'Books'),(3,'Food');
         INSERT INTO products VALUES
             (1,'Laptop', 999.99,'Electronics'),
             (2,'Tablet', 499.50,'Electronics'),
             (3,'Novel',  12.00,'Books'),
             (4,'Textbk',150.00,'Books'),
             (5,'Apple',   0.99,'Food'),
             (6,'Widget',  9.99, NULL);",
    )
    .expect("seed data");
    c
}

// Filter predicates

#[test]
fn filter_eq() {
    let mut c = mem_conn();
    let names: Vec<String> = products::table
        .filter(products::category.eq("Books"))
        .select(products::name)
        .load(&mut c)
        .unwrap();
    assert_eq!(names.len(), 2);
    assert!(names.contains(&"Novel".to_string()));
}

#[test]
fn filter_ne() {
    let mut c = mem_conn();
    let cnt: i64 =
        products::table.filter(products::category.ne("Food")).count().first(&mut c).unwrap();
    // Electronics(2) + Books(2) + NULL(1) are != "Food" (NULL is excluded by ne)
    assert_eq!(cnt, 4);
}

#[test]
fn filter_gt_lt() {
    let mut c = mem_conn();
    let cnt: i64 = products::table
        .filter(products::price.gt(100.0).and(products::price.lt(1000.0)))
        .count()
        .first(&mut c)
        .unwrap();
    assert_eq!(cnt, 3); // Laptop(999.99) + Tablet(499.50) + Textbk(150.00)
}

#[test]
fn filter_ge_le() {
    let mut c = mem_conn();
    let ids: Vec<i32> = products::table
        .filter(products::price.ge(9.99).and(products::price.le(12.0)))
        .order(products::id)
        .select(products::id)
        .load(&mut c)
        .unwrap();
    assert_eq!(ids, [3, 6]); // Novel(12.00) + Widget(9.99)
}

#[test]
fn filter_like() {
    let mut c = mem_conn();
    let names: Vec<String> = products::table
        .filter(products::name.like("T%"))
        .order(products::id)
        .select(products::name)
        .load(&mut c)
        .unwrap();
    assert_eq!(names, ["Tablet", "Textbk"]);
}

#[test]
fn filter_between() {
    let mut c = mem_conn();
    let ids: Vec<i32> = products::table
        .filter(products::price.between(9.0, 500.0))
        .order(products::price)
        .select(products::id)
        .load(&mut c)
        .unwrap();
    // Widget(9.99), Novel(12.00), Textbk(150.00), Tablet(499.50)
    assert_eq!(ids.len(), 4);
}

#[test]
fn filter_is_null() {
    let mut c = mem_conn();
    let ids: Vec<i32> = products::table
        .filter(products::category.is_null())
        .select(products::id)
        .load(&mut c)
        .unwrap();
    assert_eq!(ids, [6]); // Widget
}

#[test]
fn filter_is_not_null() {
    let mut c = mem_conn();
    let cnt: i64 =
        products::table.filter(products::category.is_not_null()).count().first(&mut c).unwrap();
    assert_eq!(cnt, 5);
}

#[test]
fn or_filter() {
    let mut c = mem_conn();
    let ids: Vec<i32> = products::table
        .filter(products::price.lt(1.0).or(products::price.gt(900.0)))
        .order(products::id)
        .select(products::id)
        .load(&mut c)
        .unwrap();
    assert_eq!(ids, [1, 5]); // Laptop(999.99), Apple(0.99)
}

// Ordering

#[test]
fn order_asc() {
    let mut c = mem_conn();
    let prices: Vec<f64> =
        products::table.order(products::price.asc()).select(products::price).load(&mut c).unwrap();
    let is_sorted = prices.windows(2).all(|w| w[0] <= w[1]);
    assert!(is_sorted);
}

#[test]
fn order_desc() {
    let mut c = mem_conn();
    let prices: Vec<f64> =
        products::table.order(products::price.desc()).select(products::price).load(&mut c).unwrap();
    let is_sorted = prices.windows(2).all(|w| w[0] >= w[1]);
    assert!(is_sorted);
}

// Limit / offset

#[test]
fn limit_returns_n_rows() {
    let mut c = mem_conn();
    let rows: Vec<i32> =
        products::table.order(products::id).limit(3).select(products::id).load(&mut c).unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn offset_skips_rows() {
    let mut c = mem_conn();
    let all: Vec<i32> =
        products::table.order(products::id).select(products::id).load(&mut c).unwrap();
    let skipped: Vec<i32> =
        products::table.order(products::id).offset(2).select(products::id).load(&mut c).unwrap();
    assert_eq!(&all[2..], skipped.as_slice());
}

// Count

#[test]
fn count_all() {
    let mut c = mem_conn();
    let cnt: i64 = products::table.count().first(&mut c).unwrap();
    assert_eq!(cnt, 6);
}

#[test]
fn count_with_filter() {
    let mut c = mem_conn();
    let cnt: i64 = products::table.filter(products::price.gt(100.0)).count().first(&mut c).unwrap();
    assert_eq!(cnt, 3); // Laptop, Tablet, Textbk
}

// Distinct

#[test]
fn distinct_categories() {
    let mut c = mem_conn();
    let cats: Vec<Option<String>> =
        products::table.select(products::category).distinct().load(&mut c).unwrap();
    // NULL + Electronics + Books + Food = 4 distinct values
    assert_eq!(cats.len(), 4);
}

// Aggregate functions

#[test]
fn aggregate_sum_price() {
    use diesel::dsl::sum;
    let mut c = mem_conn();
    let total: Option<f64> = products::table.select(sum(products::price)).first(&mut c).unwrap();
    // 999.99 + 499.50 + 12.00 + 150.00 + 0.99 + 9.99 = 1672.47
    let expected = 1_672.47;
    assert!((total.unwrap() - expected).abs() < 0.01);
}

#[test]
fn aggregate_avg_price() {
    use diesel::dsl::avg;
    let mut c = mem_conn();
    let avg_price: Option<f64> =
        products::table.select(avg(products::price)).first(&mut c).unwrap();
    assert!(avg_price.unwrap() > 0.0);
}

#[test]
fn aggregate_min_max() {
    use diesel::dsl::{max, min};
    let mut c = mem_conn();
    let min_p: Option<f64> = products::table.select(min(products::price)).first(&mut c).unwrap();
    let max_p: Option<f64> = products::table.select(max(products::price)).first(&mut c).unwrap();
    assert!((min_p.unwrap() - 0.99).abs() < 1e-9);
    assert!((max_p.unwrap() - 999.99).abs() < 1e-9);
}

// eq_any

#[test]
fn eq_any_in_list() {
    let mut c = mem_conn();
    let ids: Vec<i32> = products::table
        .filter(products::id.eq_any([1, 3, 5]))
        .order(products::id)
        .select(products::id)
        .load(&mut c)
        .unwrap();
    assert_eq!(ids, [1, 3, 5]);
}
