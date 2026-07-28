//! `FromSql`/`ToSql` implementations for DuckDB `STRUCT` / Diesel `DuckStruct`.
//!
//! The Rust mirror type is `HashMap<String, DuckValue>`, matching
//! `better_duck_core::types::value::DuckValue::Struct` exactly.

use std::collections::HashMap;

use better_duck_core::types::{value::DuckValue, value_ref::DuckValueRef};
use diesel::{
    deserialize::{self, FromSql},
    serialize::{self, IsNull, Output, ToSql},
};

use crate::backend::DuckDb;
use crate::types::duckdb_types::DuckStruct;

// FromSql

/// Deserialize a DuckDB `STRUCT` column into a [`HashMap<String, DuckValue>`].
impl FromSql<DuckStruct, DuckDb> for HashMap<String, DuckValue> {
    fn from_sql(val: DuckValueRef<'_>) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Struct(m) => {
                Ok(m.iter().map(|(k, v)| (k.clone(), DuckValue::from(v))).collect())
            },
            other => Err(format!("expected Struct, got {other:?}").into()),
        }
    }
}

// ToSql

/// Serialize a [`HashMap<String, DuckValue>`] as a DuckDB `STRUCT` bind parameter.
impl ToSql<DuckStruct, DuckDb> for HashMap<String, DuckValue> {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        // Clone each field into an owned DuckValue, then convert to a fully-owned
        // DuckValueRef (see `list.rs` for why the owned path is required here).
        let fields: HashMap<String, DuckValueRef<'b>> =
            self.iter().map(|(k, v)| (k.clone(), DuckValueRef::from(v.clone()))).collect();
        out.set_value(DuckValueRef::Struct(fields));
        Ok(IsNull::No)
    }
}
