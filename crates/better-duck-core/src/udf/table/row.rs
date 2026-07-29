//! `TableRow`: maps a Rust tuple to one output row of a table function, plus
//! the shared init-data/execution-loop helpers `#[duckdb_table_function]`
//! generates a call to.

use std::sync::Mutex;

use crate::error::Result;

use super::super::{
    logical_type::LogicalType, vector::ScalarRet, DataChunkHandle, UdfResult, VectorMut,
};

/// One output row of a table function: a tuple of values, one per column, in
/// column order.
///
/// Implemented only for tuples — never for a bare `T` — so that a
/// single-column function's `Item = T` and a multi-column function's
/// `Item = (A, B, ...)` cannot both match the same blanket impl. The
/// `#[duckdb_table_function]` macro wraps a non-tuple `Item` type in a
/// one-element tuple during code generation, so callers never need to write
/// `(T,)` themselves.
pub trait TableRow: Sized {
    /// The number of columns this row produces.
    const COLUMNS: usize;

    /// The DuckDB logical type of each column, in order.
    ///
    /// # Errors
    ///
    /// Returns an error if a column's logical type cannot be built.
    fn column_types() -> Result<Vec<LogicalType>>;

    /// Writes each element into the matching column vector at `row`.
    ///
    /// # Errors
    ///
    /// Returns an error if a value cannot be converted to a DuckDB value.
    fn write_row(
        self,
        cols: &mut [VectorMut<'_>],
        row: usize,
    ) -> UdfResult<()>;
}

macro_rules! impl_table_row {
    ($n:literal; $($T:ident : $idx:tt),+) => {
        impl<$($T: ScalarRet + crate::types::DuckLogicalType),+> TableRow for ($($T,)+) {
            const COLUMNS: usize = $n;

            fn column_types() -> Result<Vec<LogicalType>> {
                Ok(std::vec![$(LogicalType::of::<$T>()?),+])
            }

            fn write_row(self, cols: &mut [VectorMut<'_>], row: usize) -> UdfResult<()> {
                $( self.$idx.write(&mut cols[$idx], row)?; )+
                Ok(())
            }
        }
    };
}

impl_table_row!(1; A: 0);
impl_table_row!(2; A: 0, B: 1);
impl_table_row!(3; A: 0, B: 1, C: 2);
impl_table_row!(4; A: 0, B: 1, C: 2, D: 3);
impl_table_row!(5; A: 0, B: 1, C: 2, D: 3, E: 4);
impl_table_row!(6; A: 0, B: 1, C: 2, D: 3, E: 4, F: 5);
impl_table_row!(7; A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6);
impl_table_row!(8; A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7);

/// The `InitData` shape shared by every `#[duckdb_table_function]`-generated
/// `VTab` impl: a lazily consumed iterator of output rows, behind a `Mutex` so
/// it can be shared across the worker threads DuckDB may execute the scan on.
pub struct TableInitData<Row: TableRow> {
    iter: Mutex<Box<dyn Iterator<Item = Row> + Send>>,
}

impl<Row: TableRow> TableInitData<Row> {
    /// Wraps `iter` as init data for a table function scan.
    pub fn new(iter: Box<dyn Iterator<Item = Row> + Send>) -> Self {
        Self { iter: Mutex::new(iter) }
    }
}

/// Pulls up to `output.capacity()` rows from `init_data`'s iterator and writes
/// them into `output`, then sets `output`'s row count. Exhaustion is signalled
/// by writing fewer rows than the capacity (including zero), which is what
/// `DataChunkHandle::set_len` communicates to DuckDB either way.
///
/// This is the body of every `#[duckdb_table_function]`-generated `VTab::func`;
/// factoring it out here keeps generated code small.
///
/// # Errors
///
/// Returns an error if a row cannot be written.
pub fn run_table_func<Row: TableRow>(
    init_data: &TableInitData<Row>,
    output: &mut DataChunkHandle,
) -> UdfResult<()> {
    let cap = output.capacity();
    let mut written = 0usize;
    {
        // A poisoned lock only happens if a prior call panicked while holding
        // it; `contain_callback` already reports that panic as a query error,
        // so recovering the guard here just lets the (now-failing) query wind
        // down normally instead of poisoning every subsequent chunk pull too.
        let mut iter = init_data.iter.lock().unwrap_or_else(|e| e.into_inner());
        let mut cols = output.vectors_mut()?;
        while written < cap {
            let Some(item) = iter.next() else { break };
            item.write_row(&mut cols, written)?;
            written += 1;
        }
    }
    output.set_len(written)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::Connection;

    struct SumIterVTab;

    impl crate::udf::VTab for SumIterVTab {
        type BindData = ();
        type InitData = TableInitData<(i64,)>;

        fn bind(bind: &crate::udf::BindInfo) -> UdfResult<Self::BindData> {
            bind.add_result_column("n", &LogicalType::of::<i64>()?)?;
            Ok(())
        }

        fn init(_init: &crate::udf::InitInfo<Self>) -> UdfResult<Self::InitData> {
            Ok(TableInitData::new(Box::new((0i64..5).map(|v| (v,)))))
        }

        fn func(
            func: &crate::udf::TableFunctionInfo<Self>,
            output: &mut DataChunkHandle,
        ) -> UdfResult<()> {
            run_table_func(func.init_data(), output)
        }
    }

    #[test]
    fn run_table_func_drives_a_tuple_iterator_to_completion() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.register_table_function::<SumIterVTab>("sum_iter_test").unwrap();
        let mut result = conn.execute("SELECT sum(n) AS total FROM sum_iter_test()").unwrap();
        let row = result.next().unwrap().unwrap();
        assert_eq!(row.get("total"), Some(&crate::types::value::DuckValue::HugeInt(10)));
    }
}
