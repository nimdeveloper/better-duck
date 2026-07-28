//! `FromSql`/`ToSql` implementations for DuckDB `MAP` / Diesel `DuckMap`.
//!
//! The Rust mirror type is `HashMap<DuckValue, DuckValue>`, matching
//! `better_duck_core::types::value::DuckValue::Map` exactly.

use std::collections::HashMap;

use better_duck_core::types::{value::DuckValue, value_ref::DuckValueRef};
use diesel::{
    deserialize::{self, FromSql},
    serialize::{self, IsNull, Output, ToSql},
};

use crate::backend::DuckDb;
use crate::types::duckdb_types::DuckMap;

// FromSql

/// Deserialize a DuckDB `MAP` column into a [`HashMap<DuckValue, DuckValue>`].
impl FromSql<DuckMap, DuckDb> for HashMap<DuckValue, DuckValue> {
    fn from_sql(val: DuckValueRef<'_>) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Map(m) => {
                Ok(m.iter().map(|(k, v)| (DuckValue::from(k), DuckValue::from(v))).collect())
            },
            other => Err(format!("expected Map, got {other:?}").into()),
        }
    }
}

// ToSql

/// Serialize a [`HashMap<DuckValue, DuckValue>`] as a DuckDB `MAP` bind parameter.
impl ToSql<DuckMap, DuckDb> for HashMap<DuckValue, DuckValue> {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        // Clone each pair into owned DuckValues, then convert to fully-owned
        // DuckValueRefs (see `list.rs` for why the owned path is required here).
        let pairs: HashMap<DuckValueRef<'b>, DuckValueRef<'b>> = self
            .iter()
            .map(|(k, v)| (DuckValueRef::from(k.clone()), DuckValueRef::from(v.clone())))
            .collect();
        out.set_value(DuckValueRef::Map(pairs));
        Ok(IsNull::No)
    }
}
