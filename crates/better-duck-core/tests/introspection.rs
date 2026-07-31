#![allow(missing_docs)]
//! Integration tests for named parameters and schema introspection
//! (`Connection::execute_with_named`, `CachedStatement` introspection,
//! `TableDescription`).

use better_duck_core::{
    chunk::LogicalType, connection::Connection, error::Error, types::value::DuckValue,
    CachedStatement, StatementType,
};

fn conn() -> Connection {
    Connection::open_in_memory().unwrap()
}

// 1. Named-parameter bind end-to-end, bound out of declaration order.
#[test]
fn named_parameter_bind_works_out_of_declaration_order() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (a INTEGER, b VARCHAR)").unwrap();
    c.execute_batch("INSERT INTO t VALUES (1, 'x'), (2, 'y')").unwrap();

    let mut a = 1i32;
    let mut b = "x".to_string();
    let result = c
        .execute_with_named(
            "SELECT a, b FROM t WHERE b = $b AND a = $a",
            &mut [("a", &mut a), ("b", &mut b)],
        )
        .unwrap();
    let rows: Vec<_> = result.collect::<better_duck_core::error::Result<_>>().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("a"), Some(&DuckValue::Int(1)));
}

// 2. Binding an unknown parameter name -> Error::InvalidParameterName, not a
// silent no-op.
#[test]
fn binding_an_unknown_parameter_name_errors() {
    let mut c = conn();
    let mut value = 1i32;
    let err = match c.execute_with_named("SELECT $a", &mut [("nonexistent", &mut value)]) {
        Ok(_) => panic!("expected an error"),
        Err(e) => e,
    };
    assert!(matches!(err, Error::InvalidParameterName(ref n) if n == "nonexistent"), "{err}");
}

// 5. Parameter introspection: count, names, and types for a 3-parameter
// statement.
#[test]
fn parameter_introspection_reports_count_names_and_types() {
    let c = conn();
    let stmt =
        CachedStatement::prepare(c.db(), "SELECT $a::INTEGER, $b::VARCHAR, $c::DOUBLE").unwrap();
    assert_eq!(stmt.parameter_count(), 3);
    assert_eq!(stmt.parameter_name(1).as_deref(), Some("a"));
    assert_eq!(stmt.parameter_name(2).as_deref(), Some("b"));
    assert_eq!(stmt.parameter_name(3).as_deref(), Some("c"));
    assert_eq!(
        stmt.parameter_type(1).unwrap(),
        better_duck_core::ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER
    );
    assert_eq!(
        stmt.parameter_type(2).unwrap(),
        better_duck_core::ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR
    );
    assert_eq!(
        stmt.parameter_type(3).unwrap(),
        better_duck_core::ffi::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE
    );
}

// 6. Result-column metadata read before execution, with no rows fetched.
#[test]
fn result_column_metadata_is_available_before_execution() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (id INTEGER, name VARCHAR)").unwrap();
    let stmt = CachedStatement::prepare(c.db(), "SELECT id, name FROM t").unwrap();
    assert_eq!(stmt.column_count(), 2);
    assert_eq!(stmt.column_name(0).unwrap(), "id");
    assert_eq!(stmt.column_name(1).unwrap(), "name");
    assert_eq!(
        stmt.column_type(0).unwrap(),
        better_duck_core::ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER
    );
    assert_eq!(
        stmt.column_type(1).unwrap(),
        better_duck_core::ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR
    );
    let logical = stmt.column_logical_type(0).unwrap();
    assert_eq!(logical.type_id(), better_duck_core::ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER);
}

// 7. statement_type() for SELECT / INSERT / UPDATE / DELETE / CREATE, read
// from a prepared (not yet executed) statement.
#[test]
fn prepared_statement_type_identifies_each_statement_kind() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();

    let stmt = CachedStatement::prepare(c.db(), "SELECT v FROM t").unwrap();
    assert_eq!(stmt.statement_type(), StatementType::Select);

    let stmt = CachedStatement::prepare(c.db(), "INSERT INTO t VALUES (1)").unwrap();
    assert_eq!(stmt.statement_type(), StatementType::Insert);

    let stmt = CachedStatement::prepare(c.db(), "UPDATE t SET v = 2 WHERE v = 1").unwrap();
    assert_eq!(stmt.statement_type(), StatementType::Update);

    let stmt = CachedStatement::prepare(c.db(), "DELETE FROM t WHERE v = 2").unwrap();
    assert_eq!(stmt.statement_type(), StatementType::Delete);

    let stmt = CachedStatement::prepare(c.db(), "CREATE TABLE t2 (v INTEGER)").unwrap();
    assert_eq!(stmt.statement_type(), StatementType::Create);
}

// 8. TableDescription: column names and order match the DDL; column_has_default
// is true for a DEFAULT-carrying column and false otherwise.
#[test]
fn table_description_reports_columns_names_types_and_defaults() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (id INTEGER, name VARCHAR DEFAULT 'unknown', flag BOOLEAN)")
        .unwrap();
    let desc = c.table_description(None, "t").unwrap();
    assert_eq!(desc.column_count(), 3);
    assert_eq!(desc.column_name(0).unwrap(), "id");
    assert_eq!(desc.column_name(1).unwrap(), "name");
    assert_eq!(desc.column_name(2).unwrap(), "flag");
    assert_eq!(
        desc.column_type(0).unwrap().type_id(),
        better_duck_core::ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER
    );
    assert!(!desc.column_has_default(0).unwrap());
    assert!(desc.column_has_default(1).unwrap());
    assert!(!desc.column_has_default(2).unwrap());
}

// 9. TableDescription on a nonexistent table -> Err, and the error message
// names the table.
#[test]
fn table_description_on_a_nonexistent_table_errors_naming_it() {
    let c = conn();
    let err = c.table_description(None, "no_such_table").unwrap_err();
    assert!(err.to_string().contains("no_such_table"), "{err}");
}

// 10. LogicalType round-trip: build LIST<STRUCT<a INTEGER, b VARCHAR>>, read
// the child types back and confirm names/types.
#[test]
fn nested_list_of_struct_logical_type_round_trips() {
    let a = LogicalType::of::<i32>().unwrap();
    let b = LogicalType::of::<String>().unwrap();
    let inner = LogicalType::struct_of(&[("a", &a), ("b", &b)]).unwrap();
    let list = LogicalType::list_of(&inner);

    assert_eq!(list.type_id(), better_duck_core::ffi::DUCKDB_TYPE_DUCKDB_TYPE_LIST);
    let child = list.list_child_type().unwrap();
    assert_eq!(child.type_id(), better_duck_core::ffi::DUCKDB_TYPE_DUCKDB_TYPE_STRUCT);
    assert_eq!(child.struct_child_count().unwrap(), 2);
    assert_eq!(child.struct_child_name(0).unwrap(), "a");
    assert_eq!(child.struct_child_name(1).unwrap(), "b");
    assert_eq!(
        child.struct_child_type(0).unwrap().type_id(),
        better_duck_core::ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER
    );
    assert_eq!(
        child.struct_child_type(1).unwrap().type_id(),
        better_duck_core::ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR
    );
}

// 11. DECIMAL: LogicalType::decimal(18, 4) -> decimal_width() == 18, scale() == 4.
#[test]
fn decimal_logical_type_reports_width_and_scale() {
    let lt = LogicalType::decimal(18, 4).unwrap();
    assert_eq!(lt.decimal_width().unwrap(), 18);
    assert_eq!(lt.decimal_scale().unwrap(), 4);
}

// bind_decimal: a direct typed bind (via `Decimal: AppendAble`'s own
// `stmt_append`, which now calls `duckdb_bind_decimal` directly instead of
// going through the generic `duckdb_value` path) preserves width and scale
// exactly, through a named-parameter execute_with_named call.
#[cfg(feature = "decimal")]
#[test]
fn bind_decimal_round_trip_preserves_scale_through_named_bind() {
    use rust_decimal::Decimal;

    let mut c = conn();
    let mut value = Decimal::from_i128_with_scale(123_456_789, 4);
    let expected = value;
    let result =
        c.execute_with_named("SELECT $v::DECIMAL(18,4) AS r", &mut [("v", &mut value)]).unwrap();
    let rows: Vec<_> = result.collect::<better_duck_core::error::Result<_>>().unwrap();
    assert_eq!(rows.len(), 1);
    match rows[0].get("r") {
        Some(DuckValue::Decimal(got)) => assert_eq!(*got, expected),
        other => panic!("expected Decimal, got {other:?}"),
    }
}
