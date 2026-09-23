//! Introspecting `LogicalType` and its lossless `TypeInfo` descriptor.
//!
//! A [`LogicalType`] is an owned DuckDB type handle. `type_id` gives the coarse type id;
//! `alias`/`set_alias` carry a user name (a prerequisite for `register_logical_type`); and
//! `describe` materialises the full recursive [`TypeInfo`]. For nested types the handle
//! exposes its structure directly (`list_child`, `array_child`/`array_size`, `map_key`/
//! `map_value`, `struct_child*`, `union_member*`, `enum_dictionary`). Here the nested
//! handles come from an appender's `column_type`, which returns an owned `LogicalType`.

use better_duck_core::connection::Connection;
use better_duck_core::ffi::{
    DUCKDB_TYPE_DUCKDB_TYPE_ARRAY, DUCKDB_TYPE_DUCKDB_TYPE_ENUM, DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
    DUCKDB_TYPE_DUCKDB_TYPE_LIST, DUCKDB_TYPE_DUCKDB_TYPE_MAP, DUCKDB_TYPE_DUCKDB_TYPE_STRUCT,
    DUCKDB_TYPE_DUCKDB_TYPE_UNION, DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
};
use better_duck_core::types::logical_type::{LogicalType, TypeInfo};
use common::{section, show, Result};

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;

    section("a scalar type: type_id, alias, describe");
    let mut int_ty = LogicalType::of::<i32>()?;
    show("type_id", int_ty.type_id());
    show("alias (fresh)", int_ty.alias());
    show("describe", int_ty.describe());
    assert_eq!(int_ty.type_id(), DUCKDB_TYPE_DUCKDB_TYPE_INTEGER);
    assert_eq!(int_ty.alias(), None);
    assert_eq!(int_ty.describe(), TypeInfo::Scalar(DUCKDB_TYPE_DUCKDB_TYPE_INTEGER));

    section("register_logical_type requires an alias first");
    // Without an alias, registration is rejected.
    assert!(conn.register_logical_type(&int_ty).is_err());
    int_ty.set_alias("my_int")?;
    show("alias (after set_alias)", int_ty.alias());
    assert_eq!(int_ty.alias().as_deref(), Some("my_int"));
    // Now it registers, and the alias is usable as a SQL type name.
    conn.register_logical_type(&int_ty)?;
    conn.execute_batch("CREATE TABLE uses_alias (v my_int)")?;
    conn.execute_batch("INSERT INTO uses_alias VALUES (1)")?;
    let n = conn.execute("SELECT count(*) AS n FROM uses_alias")?.next().expect("row")?;
    show("rows in aliased-type table", n.get("n"));

    section("nested types via an appender's column_type");
    conn.execute_batch("CREATE TYPE mood AS ENUM ('happy', 'sad')")?;
    conn.execute_batch(
        "CREATE TABLE nested (
             lst INTEGER[],
             arr INTEGER[3],
             mp MAP(VARCHAR, INTEGER),
             st STRUCT(id INTEGER, name VARCHAR),
             un UNION(num INTEGER, txt VARCHAR),
             mood_col mood
         )",
    )?;
    let app = conn.appender("nested", "main")?;

    // LIST(INTEGER)
    let lst = app.column_type(0).expect("list type");
    show("lst type_id", lst.type_id());
    assert_eq!(lst.type_id(), DUCKDB_TYPE_DUCKDB_TYPE_LIST);
    assert_eq!(lst.list_child().expect("list child").type_id(), DUCKDB_TYPE_DUCKDB_TYPE_INTEGER);

    // ARRAY(INTEGER, 3)
    let arr = app.column_type(1).expect("array type");
    show("arr type_id / size", (arr.type_id(), arr.array_size()));
    assert_eq!(arr.type_id(), DUCKDB_TYPE_DUCKDB_TYPE_ARRAY);
    assert_eq!(arr.array_size(), 3);
    assert_eq!(arr.array_child().expect("array child").type_id(), DUCKDB_TYPE_DUCKDB_TYPE_INTEGER);

    // MAP(VARCHAR, INTEGER)
    let mp = app.column_type(2).expect("map type");
    assert_eq!(mp.type_id(), DUCKDB_TYPE_DUCKDB_TYPE_MAP);
    assert_eq!(mp.map_key().expect("map key").type_id(), DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR);
    assert_eq!(mp.map_value().expect("map value").type_id(), DUCKDB_TYPE_DUCKDB_TYPE_INTEGER);

    // STRUCT(id INTEGER, name VARCHAR)
    let st = app.column_type(3).expect("struct type");
    show("struct child_count", st.struct_child_count());
    assert_eq!(st.type_id(), DUCKDB_TYPE_DUCKDB_TYPE_STRUCT);
    assert_eq!(st.struct_child_count(), 2);
    assert_eq!(st.struct_child_name(0).as_deref(), Some("id"));
    assert_eq!(st.struct_child(0).expect("field 0").type_id(), DUCKDB_TYPE_DUCKDB_TYPE_INTEGER);
    assert_eq!(st.struct_child_name(1).as_deref(), Some("name"));

    // UNION(num INTEGER, txt VARCHAR)
    let un = app.column_type(4).expect("union type");
    show("union member_count", un.union_member_count());
    assert_eq!(un.type_id(), DUCKDB_TYPE_DUCKDB_TYPE_UNION);
    assert_eq!(un.union_member_count(), 2);
    assert_eq!(un.union_member_name(0).as_deref(), Some("num"));
    assert_eq!(un.union_member(0).expect("member 0").type_id(), DUCKDB_TYPE_DUCKDB_TYPE_INTEGER);

    // ENUM('happy', 'sad')
    let mood = app.column_type(5).expect("enum type");
    show("enum dictionary", mood.enum_dictionary());
    assert_eq!(mood.type_id(), DUCKDB_TYPE_DUCKDB_TYPE_ENUM);
    assert_eq!(mood.enum_dictionary(), vec!["happy".to_owned(), "sad".to_owned()]);

    Ok(())
}
