#![allow(missing_docs)]
//! Integration tests for aggregate function UDFs (`VAggregate`,
//! `Connection::register_aggregate_function`).

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use better_duck_core::{
    chunk::{DataChunkHandle, LogicalType, VectorMut},
    config::Config,
    connection::Connection,
    error::Result,
    types::value::DuckValue,
    udf::{AggregateSignature, VAggregate},
};

fn conn() -> Connection {
    Connection::open_in_memory().unwrap()
}

// 1. Hand-written SumInts: SELECT my_sum(v) FROM t matches SQL sum(v).
struct SumInts;

impl VAggregate for SumInts {
    type State = i64;

    fn signatures() -> Result<Vec<AggregateSignature>> {
        Ok(vec![AggregateSignature::exact(
            vec![LogicalType::of::<i32>()?],
            LogicalType::of::<i64>()?,
        )])
    }

    fn init() -> Self::State {
        0
    }

    fn update(
        state: &mut Self::State,
        input: &DataChunkHandle,
        row: usize,
    ) -> better_duck_core::udf::UdfResult<()> {
        let col = input.vector(0)?;
        let v: i32 = col.get(row)?;
        *state += i64::from(v);
        Ok(())
    }

    fn combine(
        source: &Self::State,
        target: &mut Self::State,
    ) -> better_duck_core::udf::UdfResult<()> {
        *target += *source;
        Ok(())
    }

    fn finalize(
        state: &Self::State,
        output: &mut VectorMut<'_>,
        row: usize,
    ) -> better_duck_core::udf::UdfResult<()> {
        output.set(row, *state)?;
        Ok(())
    }
}

#[test]
fn my_sum_matches_sql_sum() {
    let mut c = conn();
    c.register_aggregate_function::<SumInts>("my_sum").unwrap();
    c.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
    c.execute_batch("INSERT INTO t VALUES (1), (2), (3), (100)").unwrap();
    let mut result = c.execute("SELECT my_sum(v) AS r FROM t").unwrap();
    let row = result.next().unwrap().unwrap();
    assert_eq!(row.get("r"), Some(&DuckValue::BigInt(106)));
}

// 2. GROUP BY over 10 groups — exercises many concurrent states and combine.
#[test]
fn group_by_ten_groups_matches_sql_sum_per_group() {
    let mut c = conn();
    c.register_aggregate_function::<SumInts>("my_sum").unwrap();
    c.execute_batch(
        "CREATE TABLE t AS SELECT (i % 10)::INTEGER AS k, i::INTEGER AS v FROM range(1000) t(i)",
    )
    .unwrap();

    let result = c
        .execute("SELECT k, my_sum(v) AS mine, sum(v) AS truth FROM t GROUP BY k ORDER BY k")
        .unwrap();
    let rows: Vec<_> = result.collect::<Result<_>>().unwrap();
    assert_eq!(rows.len(), 10);
    for row in &rows {
        let mine = match row.get("mine") {
            Some(DuckValue::BigInt(n)) => *n,
            other => panic!("expected BigInt, got {other:?}"),
        };
        let truth = match row.get("truth") {
            Some(DuckValue::HugeInt(n)) => *n as i64,
            other => panic!("expected HugeInt, got {other:?}"),
        };
        assert_eq!(mine, truth);
    }
}

// 3. 5000-row input (multi-chunk) — exercises repeated update into one state.
#[test]
fn five_thousand_rows_spanning_multiple_chunks_sums_correctly() {
    let mut c = conn();
    c.register_aggregate_function::<SumInts>("my_sum").unwrap();
    c.execute_batch("CREATE TABLE t AS SELECT (i % 1000)::INTEGER AS v FROM range(5000) t(i)")
        .unwrap();
    let mut result = c.execute("SELECT my_sum(v) AS r FROM t").unwrap();
    let row = result.next().unwrap().unwrap();
    let expected: i64 = (0..5000i64).map(|i| i % 1000).sum();
    assert_eq!(row.get("r"), Some(&DuckValue::BigInt(expected)));
}

// 4. set_max_threads(4) + large input — exercises combine across worker
// threads; results exact.
#[test]
fn multi_threaded_execution_combines_partial_states_exactly() {
    let config = Config::default().threads(4).unwrap();
    let mut c = Connection::open_in_memory_with_flags(config).unwrap();
    c.register_aggregate_function::<SumInts>("my_sum").unwrap();
    c.execute_batch("CREATE TABLE t AS SELECT 1::INTEGER AS v FROM range(200000)").unwrap();
    let mut result = c.execute("SELECT my_sum(v) AS r FROM t").unwrap();
    let row = result.next().unwrap().unwrap();
    assert_eq!(row.get("r"), Some(&DuckValue::BigInt(200000)));
}

// 5. NULL handling: `update` is invoked for *every* row, `NULL` parameters
// included — unlike a `VScalar`, DuckDB does not pre-filter NULL-argument
// rows out of an aggregate's input. An aggregate that wants to skip them
// must check `VectorRef::is_null` itself.
struct CountAllRows;

impl VAggregate for CountAllRows {
    type State = i64;

    fn signatures() -> Result<Vec<AggregateSignature>> {
        Ok(vec![AggregateSignature::exact(
            vec![LogicalType::of::<Option<i32>>()?],
            LogicalType::of::<i64>()?,
        )])
    }

    fn init() -> Self::State {
        0
    }

    fn update(
        state: &mut Self::State,
        _input: &DataChunkHandle,
        _row: usize,
    ) -> better_duck_core::udf::UdfResult<()> {
        *state += 1;
        Ok(())
    }

    fn combine(
        source: &Self::State,
        target: &mut Self::State,
    ) -> better_duck_core::udf::UdfResult<()> {
        *target += *source;
        Ok(())
    }

    fn finalize(
        state: &Self::State,
        output: &mut VectorMut<'_>,
        row: usize,
    ) -> better_duck_core::udf::UdfResult<()> {
        output.set(row, *state)?;
        Ok(())
    }
}

struct CountNonNullRows;

impl VAggregate for CountNonNullRows {
    type State = i64;

    fn signatures() -> Result<Vec<AggregateSignature>> {
        Ok(vec![AggregateSignature::exact(
            vec![LogicalType::of::<Option<i32>>()?],
            LogicalType::of::<i64>()?,
        )])
    }

    fn init() -> Self::State {
        0
    }

    fn update(
        state: &mut Self::State,
        input: &DataChunkHandle,
        row: usize,
    ) -> better_duck_core::udf::UdfResult<()> {
        if !input.vector(0)?.is_null(row) {
            *state += 1;
        }
        Ok(())
    }

    fn combine(
        source: &Self::State,
        target: &mut Self::State,
    ) -> better_duck_core::udf::UdfResult<()> {
        *target += *source;
        Ok(())
    }

    fn finalize(
        state: &Self::State,
        output: &mut VectorMut<'_>,
        row: usize,
    ) -> better_duck_core::udf::UdfResult<()> {
        output.set(row, *state)?;
        Ok(())
    }
}

#[test]
fn update_is_invoked_for_every_row_including_nulls() {
    let mut c = conn();
    c.register_aggregate_function::<CountAllRows>("count_all").unwrap();
    c.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
    c.execute_batch("INSERT INTO t VALUES (1), (NULL), (2), (NULL), (3)").unwrap();
    let mut result = c.execute("SELECT count_all(v) AS r FROM t").unwrap();
    let row = result.next().unwrap().unwrap();
    assert_eq!(
        row.get("r"),
        Some(&DuckValue::BigInt(5)),
        "update must be invoked for every row, including NULLs"
    );
}

#[test]
fn manual_is_null_check_lets_update_skip_null_rows() {
    let mut c = conn();
    c.register_aggregate_function::<CountNonNullRows>("count_non_null").unwrap();
    c.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
    c.execute_batch("INSERT INTO t VALUES (1), (NULL), (2), (NULL), (3)").unwrap();
    let mut result = c.execute("SELECT count_non_null(v) AS r FROM t").unwrap();
    let row = result.next().unwrap().unwrap();
    assert_eq!(
        row.get("r"),
        Some(&DuckValue::BigInt(3)),
        "a manual is_null check inside update must correctly skip NULL rows"
    );
}

// 6. Empty input -> finalize on a fresh state produces the documented
// identity value.
#[test]
fn empty_input_finalizes_the_fresh_init_state() {
    let mut c = conn();
    c.register_aggregate_function::<SumInts>("my_sum").unwrap();
    c.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
    let mut result = c.execute("SELECT my_sum(v) AS r FROM t").unwrap();
    let row = result.next().unwrap().unwrap();
    assert_eq!(row.get("r"), Some(&DuckValue::BigInt(0)), "identity value for an empty sum is 0");
}

// 7. Err from update -> query error, connection usable afterward.
struct FailsOnUpdate;

impl VAggregate for FailsOnUpdate {
    type State = ();

    fn signatures() -> Result<Vec<AggregateSignature>> {
        Ok(vec![AggregateSignature::exact(
            vec![LogicalType::of::<i32>()?],
            LogicalType::of::<i32>()?,
        )])
    }

    fn init() -> Self::State {}

    fn update(
        _state: &mut Self::State,
        _input: &DataChunkHandle,
        _row: usize,
    ) -> better_duck_core::udf::UdfResult<()> {
        Err("update always fails".into())
    }

    fn combine(
        _source: &Self::State,
        _target: &mut Self::State,
    ) -> better_duck_core::udf::UdfResult<()> {
        Ok(())
    }

    fn finalize(
        _state: &Self::State,
        output: &mut VectorMut<'_>,
        row: usize,
    ) -> better_duck_core::udf::UdfResult<()> {
        output.set(row, 0i32)?;
        Ok(())
    }
}

#[test]
fn update_error_surfaces_as_query_error_and_connection_stays_usable() {
    let mut c = conn();
    c.register_aggregate_function::<FailsOnUpdate>("fails_on_update").unwrap();
    c.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
    c.execute_batch("INSERT INTO t VALUES (1)").unwrap();
    let err = match c.execute("SELECT fails_on_update(v) FROM t") {
        Ok(_) => panic!("expected an error"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("update always fails"), "{err}");
    c.execute_batch("INSERT INTO t VALUES (2)").unwrap();
}

// 8. Err from finalize -> query error, connection usable afterward.
struct FailsOnFinalize;

impl VAggregate for FailsOnFinalize {
    type State = ();

    fn signatures() -> Result<Vec<AggregateSignature>> {
        Ok(vec![AggregateSignature::exact(
            vec![LogicalType::of::<i32>()?],
            LogicalType::of::<i32>()?,
        )])
    }

    fn init() -> Self::State {}

    fn update(
        _state: &mut Self::State,
        _input: &DataChunkHandle,
        _row: usize,
    ) -> better_duck_core::udf::UdfResult<()> {
        Ok(())
    }

    fn combine(
        _source: &Self::State,
        _target: &mut Self::State,
    ) -> better_duck_core::udf::UdfResult<()> {
        Ok(())
    }

    fn finalize(
        _state: &Self::State,
        _output: &mut VectorMut<'_>,
        _row: usize,
    ) -> better_duck_core::udf::UdfResult<()> {
        Err("finalize always fails".into())
    }
}

#[test]
fn finalize_error_surfaces_as_query_error_and_connection_stays_usable() {
    let mut c = conn();
    c.register_aggregate_function::<FailsOnFinalize>("fails_on_finalize").unwrap();
    c.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
    c.execute_batch("INSERT INTO t VALUES (1)").unwrap();
    let err = match c.execute("SELECT fails_on_finalize(v) FROM t") {
        Ok(_) => panic!("expected an error"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("finalize always fails"), "{err}");
    c.execute_batch("INSERT INTO t VALUES (2)").unwrap();
}

// 9. Panic in update -> contained, surfaces as query error.
struct PanicsOnUpdate;

impl VAggregate for PanicsOnUpdate {
    type State = ();

    fn signatures() -> Result<Vec<AggregateSignature>> {
        Ok(vec![AggregateSignature::exact(
            vec![LogicalType::of::<i32>()?],
            LogicalType::of::<i32>()?,
        )])
    }

    fn init() -> Self::State {}

    fn update(
        _state: &mut Self::State,
        _input: &DataChunkHandle,
        _row: usize,
    ) -> better_duck_core::udf::UdfResult<()> {
        panic!("deliberate update panic")
    }

    fn combine(
        _source: &Self::State,
        _target: &mut Self::State,
    ) -> better_duck_core::udf::UdfResult<()> {
        Ok(())
    }

    fn finalize(
        _state: &Self::State,
        output: &mut VectorMut<'_>,
        row: usize,
    ) -> better_duck_core::udf::UdfResult<()> {
        output.set(row, 0i32)?;
        Ok(())
    }
}

#[test]
#[cfg(panic = "unwind")]
fn update_panic_is_contained_and_connection_stays_usable() {
    let mut c = conn();
    c.register_aggregate_function::<PanicsOnUpdate>("panics_on_update").unwrap();
    c.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
    c.execute_batch("INSERT INTO t VALUES (1)").unwrap();
    let err = match c.execute("SELECT panics_on_update(v) FROM t") {
        Ok(_) => panic!("expected an error"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("deliberate update panic"), "{err}");
    c.execute_batch("INSERT INTO t VALUES (2)").unwrap();
}

// 10. Destructor runs exactly once per state: State holding an
// Arc<AtomicUsize> drop counter; after the query, dropped count == states
// created.
struct DropCounted(Arc<AtomicUsize>);

impl Drop for DropCounted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

static CREATED: AtomicUsize = AtomicUsize::new(0);
static DROP_COUNTER: std::sync::OnceLock<Arc<AtomicUsize>> = std::sync::OnceLock::new();

struct DropTracking;

impl VAggregate for DropTracking {
    type State = DropCounted;

    fn signatures() -> Result<Vec<AggregateSignature>> {
        Ok(vec![AggregateSignature::exact(
            vec![LogicalType::of::<i32>()?],
            LogicalType::of::<i32>()?,
        )])
    }

    fn init() -> Self::State {
        CREATED.fetch_add(1, Ordering::SeqCst);
        let counter = DROP_COUNTER.get_or_init(|| Arc::new(AtomicUsize::new(0)));
        DropCounted(counter.clone())
    }

    fn update(
        _state: &mut Self::State,
        _input: &DataChunkHandle,
        _row: usize,
    ) -> better_duck_core::udf::UdfResult<()> {
        Ok(())
    }

    fn combine(
        _source: &Self::State,
        _target: &mut Self::State,
    ) -> better_duck_core::udf::UdfResult<()> {
        Ok(())
    }

    fn finalize(
        _state: &Self::State,
        output: &mut VectorMut<'_>,
        row: usize,
    ) -> better_duck_core::udf::UdfResult<()> {
        output.set(row, 0i32)?;
        Ok(())
    }
}

#[test]
fn destructor_runs_exactly_once_per_state() {
    let mut c = conn();
    c.register_aggregate_function::<DropTracking>("drop_tracking").unwrap();
    c.execute_batch(
        "CREATE TABLE t AS SELECT (i % 20)::INTEGER AS k, i::INTEGER AS v FROM range(500) t(i)",
    )
    .unwrap();

    let result = c.execute("SELECT k, drop_tracking(v) AS r FROM t GROUP BY k").unwrap();
    let rows: Vec<_> = result.collect::<Result<_>>().unwrap();
    assert_eq!(rows.len(), 20);
    drop(c);

    let created = CREATED.load(Ordering::SeqCst);
    let dropped = DROP_COUNTER.get().unwrap().load(Ordering::SeqCst);
    assert!(created > 0);
    assert_eq!(created, dropped, "every created state must be dropped exactly once");
}

// 11. Aggregate function set with two overloads (i32 and i64) dispatches to
// the right one.
struct SumEither;

impl VAggregate for SumEither {
    type State = i64;

    fn signatures() -> Result<Vec<AggregateSignature>> {
        Ok(vec![
            AggregateSignature::exact(vec![LogicalType::of::<i32>()?], LogicalType::of::<i64>()?),
            AggregateSignature::exact(vec![LogicalType::of::<i64>()?], LogicalType::of::<i64>()?),
        ])
    }

    fn init() -> Self::State {
        0
    }

    fn update(
        state: &mut Self::State,
        input: &DataChunkHandle,
        row: usize,
    ) -> better_duck_core::udf::UdfResult<()> {
        // `get::<T>` trusts the caller that `T` matches the column's actual
        // physical type (it's a raw pointer-cast read, not a checked
        // conversion) — so which overload is active must be determined from
        // the column's real type, not by trying one type and catching a
        // (nonexistent) error from a mismatched one.
        let col = input.vector(0)?;
        let v: i64 =
            if input.column_type_id(0)? == better_duck_core::ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT {
                col.get::<i64>(row)?
            } else {
                i64::from(col.get::<i32>(row)?)
            };
        *state += v;
        Ok(())
    }

    fn combine(
        source: &Self::State,
        target: &mut Self::State,
    ) -> better_duck_core::udf::UdfResult<()> {
        *target += *source;
        Ok(())
    }

    fn finalize(
        state: &Self::State,
        output: &mut VectorMut<'_>,
        row: usize,
    ) -> better_duck_core::udf::UdfResult<()> {
        output.set(row, *state)?;
        Ok(())
    }
}

#[test]
fn overload_set_dispatches_to_the_matching_signature() {
    let mut c = conn();
    c.register_aggregate_function::<SumEither>("sum_either").unwrap();
    c.execute_batch("CREATE TABLE t32 (v INTEGER)").unwrap();
    c.execute_batch("INSERT INTO t32 VALUES (1), (2), (3)").unwrap();
    c.execute_batch("CREATE TABLE t64 (v BIGINT)").unwrap();
    c.execute_batch("INSERT INTO t64 VALUES (10), (20), (30)").unwrap();

    let mut r32 = c.execute("SELECT sum_either(v) AS r FROM t32").unwrap();
    assert_eq!(r32.next().unwrap().unwrap().get("r"), Some(&DuckValue::BigInt(6)));

    let mut r64 = c.execute("SELECT sum_either(v) AS r FROM t64").unwrap();
    assert_eq!(r64.next().unwrap().unwrap().get("r"), Some(&DuckValue::BigInt(60)));
}

// 12. A non-trivial state (Vec<String> — heap-owning, non-Copy) round-trips
// through init/update/combine/finalize without leaking.
struct ConcatStrings;

impl VAggregate for ConcatStrings {
    type State = Vec<String>;

    fn signatures() -> Result<Vec<AggregateSignature>> {
        Ok(vec![AggregateSignature::exact(
            vec![LogicalType::of::<String>()?],
            LogicalType::of::<String>()?,
        )])
    }

    fn init() -> Self::State {
        Vec::new()
    }

    fn update(
        state: &mut Self::State,
        input: &DataChunkHandle,
        row: usize,
    ) -> better_duck_core::udf::UdfResult<()> {
        let col = input.vector(0)?;
        let v: String = col.get(row)?;
        state.push(v);
        Ok(())
    }

    fn combine(
        source: &Self::State,
        target: &mut Self::State,
    ) -> better_duck_core::udf::UdfResult<()> {
        target.extend(source.iter().cloned());
        Ok(())
    }

    fn finalize(
        state: &Self::State,
        output: &mut VectorMut<'_>,
        row: usize,
    ) -> better_duck_core::udf::UdfResult<()> {
        let mut sorted = state.clone();
        sorted.sort();
        output.set(row, sorted.join(","))?;
        Ok(())
    }
}

#[test]
fn non_trivial_heap_owning_state_round_trips_without_leaking() {
    let mut c = conn();
    c.register_aggregate_function::<ConcatStrings>("concat_strs").unwrap();
    c.execute_batch("CREATE TABLE t (v VARCHAR)").unwrap();
    c.execute_batch("INSERT INTO t VALUES ('c'), ('a'), ('b')").unwrap();
    let mut result = c.execute("SELECT concat_strs(v) AS r FROM t").unwrap();
    let row = result.next().unwrap().unwrap();
    assert_eq!(row.get("r"), Some(&DuckValue::text("a,b,c")));
}
