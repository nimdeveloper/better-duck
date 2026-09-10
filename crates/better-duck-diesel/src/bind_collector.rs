use better_duck_core::types::{value::DuckValue, value_ref::DuckValueRef};
use diesel::{
    query_builder::MoveableBindCollector,
    serialize::{IsNull, Output},
    sql_types::{HasSqlType, TypeMetadata},
    QueryResult,
};

use crate::backend::DuckDb;

use crate::types::*;

/// Collects bind parameters produced by Diesel's query DSL for a single query execution.
#[derive(Default)]
pub struct DuckDbBindCollector<'a> {
    pub(crate) binds: Vec<DuckValueRef<'a>>,
    // pub(crate) metadata: Vec<DuckDbType>,
}

impl<'a> diesel::query_builder::bind_collector::BindCollector<'a, DuckDb>
    for DuckDbBindCollector<'a>
{
    type Buffer = DuckValueRef<'a>;

    fn push_bound_value<T, U>(
        &mut self,
        bind: &'a U,
        metadata_lookup: &mut <DuckDb as TypeMetadata>::MetadataLookup,
    ) -> QueryResult<()>
    where
        DuckDb: HasSqlType<T>,
        U: ToSql<T, DuckDb> + ?Sized,
    {
        let value = DuckValueRef::Null;
        let mut to_sql_output = Output::new(value, metadata_lookup);
        let is_null =
            bind.to_sql(&mut to_sql_output).map_err(diesel::result::Error::SerializationError)?;
        let bind = to_sql_output.into_inner();
        // let metadata = DuckDb::metadata(metadata_lookup);
        self.binds.push(match is_null {
            IsNull::No => bind,
            IsNull::Yes => DuckValueRef::Null,
        });
        Ok(())

        // let mut data: Vec<duckdb::types::Value> = Vec::new();
        // let metadata = <DuckDb as HasSqlType<T>>::metadata(metadata_lookup);
        // data.push(bind.to_sql());
        // self.metadata.push(metadata);
        // Ok(())
    }

    fn push_null_value(
        &mut self,
        _metadata: <DuckDb as TypeMetadata>::TypeMetadata,
    ) -> QueryResult<()> {
        // self.metadata.push(metadata);
        self.binds.push(DuckValueRef::Null);
        Ok(())
    }

    // fn push_bound_value<T, U>(
    //     &mut self,
    //     bind: &'a U,
    //     metadata_lookup: &mut <DuckDb as TypeMetadata>::MetadataLookup,
    // ) -> QueryResult<()>
    // where
    //     U: diesel::serialize::ToSql<T, DuckDb> + ?Sized + 'a,
    // {
    //     let value = diesel::serialize::ToSql::<T, DuckDb>::to_sql(bind, metadata_lookup)?;
    //     self.binds.push(value);
    //     Ok(())
    // }
}

#[derive(Debug)]
pub struct DuckDbBindCollectorData {
    binds: Vec<DuckValue>,
}

impl MoveableBindCollector<DuckDb> for DuckDbBindCollector<'_> {
    type BindData = DuckDbBindCollectorData;

    fn moveable(&self) -> Self::BindData {
        let mut binds = Vec::with_capacity(self.binds.len());
        for b in self.binds.iter().map(DuckValue::from) {
            binds.push(b);
        }
        DuckDbBindCollectorData { binds }
    }

    fn append_bind_data(
        &mut self,
        from: &Self::BindData,
    ) {
        self.binds.reserve_exact(from.binds.len());
        // Clone each DuckValue and convert via From<DuckValue> for DuckValueRef<'a>.
        // The owned-input From<DuckValue> impl produces fully-owned Cow::Owned data, so
        // Rust infers the bind-collector's lifetime 'a from the Vec context — no
        // 'static / invariance issue.
        self.binds.extend(from.binds.iter().cloned().map(DuckValueRef::from));
    }

    /// Pushes each bind's `Debug` representation, used by `debug_query`/`EXPLAIN` output.
    fn push_debug_binds<'a, 'b>(
        bind_data: &Self::BindData,
        f: &'a mut Vec<Box<dyn std::fmt::Debug + 'b>>,
    ) {
        f.extend(bind_data.binds.iter().map(|b| Box::new(b.clone()) as Box<dyn std::fmt::Debug>));
    }
}

#[cfg(test)]
mod tests {
    use better_duck_core::types::{value::DuckValue, value_ref::DuckValueRef, Type};
    use diesel::query_builder::{BindCollector, MoveableBindCollector};

    use super::{DuckDbBindCollector, DuckDbBindCollectorData};
    use crate::backend::DuckDbTypeWrapper;

    #[test]
    fn push_null_value_appends_null_bind() {
        let mut collector = DuckDbBindCollector::default();
        collector.push_null_value(DuckDbTypeWrapper(Type::Int)).unwrap();
        assert_eq!(collector.binds, [DuckValueRef::Null]);
    }

    #[test]
    fn moveable_owns_borrowed_values() {
        let text = String::from("borrowed");
        let collector =
            DuckDbBindCollector { binds: vec![DuckValueRef::Text(text.as_str().into())] };
        let data = collector.moveable();
        drop(text);
        assert_eq!(data.binds, [DuckValue::text("borrowed")]);
    }

    #[test]
    fn append_bind_data_preserves_values_and_order() {
        let data = DuckDbBindCollectorData {
            binds: vec![DuckValue::Int(1), DuckValue::text("two"), DuckValue::Null],
        };
        let mut collector = DuckDbBindCollector::default();
        collector.append_bind_data(&data);
        assert_eq!(
            collector.binds,
            [DuckValueRef::Int(1), DuckValueRef::Text("two".into()), DuckValueRef::Null,]
        );
    }

    #[test]
    fn push_debug_binds_emits_one_representation_per_bind() {
        let data =
            DuckDbBindCollectorData { binds: vec![DuckValue::Int(7), DuckValue::text("duck")] };
        let mut debug_binds = Vec::new();
        DuckDbBindCollector::push_debug_binds(&data, &mut debug_binds);
        let rendered: Vec<String> = debug_binds.iter().map(|bind| format!("{bind:?}")).collect();
        assert_eq!(rendered, ["Int(7)", "Text(\"duck\")"]);
    }
}
