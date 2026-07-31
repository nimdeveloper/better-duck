//! DuckDB aggregate functions: custom SQL aggregates evaluated per group,
//! e.g. `SELECT my_agg(x) FROM t GROUP BY k`.

mod function;

use std::{ffi::CString, mem};

use crate::{
    connection::Connection,
    error::Result,
    ffi::{duckdb_aggregate_state, duckdb_data_chunk, duckdb_function_info, duckdb_vector, idx_t},
};

use self::function::{AggregateFunction, AggregateFunctionInfo, AggregateFunctionSet};
use super::{
    callback::{contain_callback, drop_or_report},
    DataChunkHandle, LogicalType, VectorMut,
};

/// A DuckDB aggregate function: accumulates a value across every row of a
/// group, then produces one result per group.
///
/// See the callback containment contract in [`crate::udf`].
///
/// # Design: why `update`/`finalize` are row-at-a-time, not batched
///
/// A tempting signature for [`update`](Self::update) is a whole batch at
/// once — `fn update(states: &mut [&mut Self::State], input: &DataChunkHandle)`
/// — mirroring [`VScalar::invoke`](super::super::VScalar::invoke)'s
/// whole-chunk access. That signature is unsound: DuckDB's grouped
/// aggregation hands `update` one state pointer *per row*, and rows
/// belonging to the same group share the same pointer — building a slice of
/// `&mut Self::State` from that array would alias two mutable references at
/// the same address whenever two rows of a chunk share a group, which is the
/// common case for any real `GROUP BY`. Taking one row at a time instead
/// means at most one `&mut Self::State` for a given group ever exists at a
/// time, even though the *same* group's state is (soundly) re-borrowed
/// across successive calls.
pub trait VAggregate: Sized {
    /// Per-group accumulator state. Never exposed to DuckDB directly — this
    /// crate stores it behind a heap allocation reached through the raw
    /// per-group memory DuckDB allocates, so it need not have any particular
    /// size or alignment DuckDB's own allocator would otherwise have to
    /// guarantee.
    type State: Send + Sync + 'static;

    /// The possible signatures of this function. Each becomes a DuckDB
    /// overload; every other method must be able to handle every one of them.
    ///
    /// # Errors
    ///
    /// Returns an error if a signature's logical type cannot be built.
    fn signatures() -> Result<Vec<AggregateSignature>>;

    /// Produces a fresh accumulator for a new group.
    fn init() -> Self::State;

    /// Folds the value(s) at `row` of `input` into `state`.
    ///
    /// # Errors
    ///
    /// Returns an error to fail the query with that message.
    fn update(
        state: &mut Self::State,
        input: &DataChunkHandle,
        row: usize,
    ) -> super::UdfResult<()>;

    /// Merges `source` (a partial accumulation, e.g. from another worker
    /// thread) into `target`.
    ///
    /// # Errors
    ///
    /// Returns an error to fail the query with that message.
    fn combine(
        source: &Self::State,
        target: &mut Self::State,
    ) -> super::UdfResult<()>;

    /// Writes `state`'s final value into `output` at `row`.
    ///
    /// # Errors
    ///
    /// Returns an error to fail the query with that message.
    fn finalize(
        state: &Self::State,
        output: &mut VectorMut<'_>,
        row: usize,
    ) -> super::UdfResult<()>;

    /// Unlike a `VScalar`, [`update`](Self::update) is **always** invoked for
    /// every row, `NULL` parameters included — DuckDB does not pre-filter
    /// `NULL`-argument rows out of an aggregate's input the way it does for
    /// scalar functions. Check
    /// [`VectorRef::is_null`](super::super::VectorRef::is_null) inside
    /// `update` if `NULL` inputs need different treatment.
    ///
    /// This flag instead tells the query planner whether the aggregate
    /// *propagates* `NULL`s the way most expressions do (`false`, the
    /// default) or not (`true`) — it can affect planner-level rewrites, but
    /// never which rows `update` sees.
    fn special_handling() -> bool {
        false
    }
}

/// One overload of an aggregate function: a parameter shape and a return type.
pub struct AggregateSignature {
    parameters: Vec<LogicalType>,
    return_type: LogicalType,
}

impl AggregateSignature {
    /// A signature with a fixed list of parameter types.
    pub fn exact(
        parameters: Vec<LogicalType>,
        return_type: LogicalType,
    ) -> Self {
        Self { parameters, return_type }
    }

    fn apply(
        &self,
        f: &AggregateFunction,
    ) {
        f.set_return_type(&self.return_type);
        for p in &self.parameters {
            f.add_parameter(p);
        }
    }
}

/// Reads the boxed `S::State` pointer out of a raw per-group memory slot.
///
/// # Safety
///
/// `slot` must point to `size_of::<*mut S::State>()` live bytes previously
/// written by [`aggregate_init_trampoline`] with a pointer from
/// `Box::into_raw::<S::State>`, not yet freed.
unsafe fn boxed_state_ptr<S: VAggregate>(slot: duckdb_aggregate_state) -> *mut S::State {
    // SAFETY: see function doc comment.
    unsafe { *slot.cast::<*mut S::State>() }
}

/// The C trampoline installed as `state_size` via
/// `duckdb_aggregate_function_set_functions`.
///
/// Reports a constant (the size of one pointer) regardless of `S::State`'s
/// actual size or alignment — see [`VAggregate`]'s docs for why. This can
/// neither panic nor fail, so unlike every other trampoline here it does not
/// go through [`contain_callback`].
unsafe extern "C" fn aggregate_state_size_trampoline<S: VAggregate>(
    _info: duckdb_function_info
) -> idx_t {
    mem::size_of::<*mut S::State>() as idx_t
}

/// The C trampoline installed as `state_init`.
///
/// Nothing above the `contain_callback` call may panic or carry a `Drop`
/// impl. See [`crate::udf`] for the full `panic = "abort"` caveat.
unsafe extern "C" fn aggregate_init_trampoline<S: VAggregate>(
    info: duckdb_function_info,
    state: duckdb_aggregate_state,
) {
    let sink = AggregateFunctionInfo::from(info);
    contain_callback(&sink, || {
        let boxed = Box::into_raw(Box::new(S::init()));
        // SAFETY: `state` points to `size_of::<*mut S::State>()` live bytes
        // that DuckDB just allocated for this group's state — exactly the
        // size `aggregate_state_size_trampoline` reported.
        unsafe { state.cast::<*mut S::State>().write(boxed) };
        Ok(())
    });
}

/// The C trampoline installed as `update`.
unsafe extern "C" fn aggregate_update_trampoline<S: VAggregate>(
    info: duckdb_function_info,
    input: duckdb_data_chunk,
    states: *mut duckdb_aggregate_state,
) {
    let sink = AggregateFunctionInfo::from(info);
    contain_callback(&sink, || {
        // SAFETY: DuckDB owns `input` and guarantees it stays live and
        // unmutated by other code for the duration of this call.
        let chunk = unsafe { DataChunkHandle::borrowed(input) };
        for row in 0..chunk.len() {
            // SAFETY: `states` is an array of `chunk.len()` valid per-row
            // state-slot pointers, guaranteed live by DuckDB for this call.
            let slot = unsafe { *states.add(row) };
            // SAFETY: `slot` was written by `aggregate_init_trampoline::<S>`
            // for the group this row belongs to, and is not concurrently
            // accessed — DuckDB serializes calls to this trampoline.
            let state = unsafe { &mut *boxed_state_ptr::<S>(slot) };
            S::update(state, &chunk, row)?;
        }
        Ok(())
    });
}

/// The C trampoline installed as `combine`.
unsafe extern "C" fn aggregate_combine_trampoline<S: VAggregate>(
    info: duckdb_function_info,
    source: *mut duckdb_aggregate_state,
    target: *mut duckdb_aggregate_state,
    count: idx_t,
) {
    let sink = AggregateFunctionInfo::from(info);
    contain_callback(&sink, || {
        for i in 0..count as usize {
            // SAFETY: `source`/`target` are arrays of `count` valid per-group
            // state-slot pointers, guaranteed live by DuckDB for this call.
            let (source_slot, target_slot) = unsafe { (*source.add(i), *target.add(i)) };
            // SAFETY: both slots were written by `aggregate_init_trampoline::<S>`;
            // `source` and `target` name distinct groups, so these do not alias.
            let source_state = unsafe { &*boxed_state_ptr::<S>(source_slot) };
            // SAFETY: see above.
            let target_state = unsafe { &mut *boxed_state_ptr::<S>(target_slot) };
            S::combine(source_state, target_state)?;
        }
        Ok(())
    });
}

/// The C trampoline installed as `finalize`.
unsafe extern "C" fn aggregate_finalize_trampoline<S: VAggregate>(
    info: duckdb_function_info,
    source: *mut duckdb_aggregate_state,
    result: duckdb_vector,
    count: idx_t,
    offset: idx_t,
) {
    let sink = AggregateFunctionInfo::from(info);
    contain_callback(&sink, || {
        // SAFETY: DuckDB owns `result`, guarantees it stays live for the
        // duration of this call, and that no other code writes through it
        // concurrently.
        let mut out = unsafe { VectorMut::new(result) };
        for i in 0..count as usize {
            // SAFETY: `source` is an array of `count` valid per-group
            // state-slot pointers, guaranteed live by DuckDB for this call.
            let slot = unsafe { *source.add(i) };
            // SAFETY: `slot` was written by `aggregate_init_trampoline::<S>`.
            let state = unsafe { &*boxed_state_ptr::<S>(slot) };
            S::finalize(state, &mut out, offset as usize + i)?;
        }
        Ok(())
    });
}

/// The C trampoline installed as the state destructor.
///
/// Has no `duckdb_function_info`, so unlike every other trampoline here it
/// cannot report a failure through DuckDB — a panicking `Drop` impl on
/// `S::State` is instead contained and logged by
/// [`drop_or_report`](super::callback::drop_or_report), matching how a
/// panicking destructor is handled everywhere else in this module.
unsafe extern "C" fn aggregate_destroy_trampoline<S: VAggregate>(
    states: *mut duckdb_aggregate_state,
    count: idx_t,
) {
    for i in 0..count as usize {
        // SAFETY: `states` is an array of `count` valid per-group state-slot
        // pointers, guaranteed live by DuckDB for this call.
        let slot = unsafe { *states.add(i) };
        // SAFETY: `slot` was written by `aggregate_init_trampoline::<S>` with
        // a pointer from `Box::into_raw::<S::State>`, not yet freed — this is
        // DuckDB's one-time teardown call for this group's state.
        let boxed = unsafe { Box::from_raw(boxed_state_ptr::<S>(slot)) };
        drop_or_report(std::any::type_name::<S::State>(), boxed);
    }
}

impl Connection {
    /// Registers `S` as an aggregate function named `name`.
    ///
    /// # Errors
    ///
    /// Returns an error if `name` contains a NUL byte, a signature's logical
    /// type cannot be built, or DuckDB rejects the registration (e.g. a name
    /// conflict).
    pub fn register_aggregate_function<S: VAggregate>(
        &mut self,
        name: &str,
    ) -> Result<()> {
        let c_name = CString::new(name)?;
        let set = AggregateFunctionSet::new(&c_name);
        for signature in S::signatures()? {
            let f = AggregateFunction::new(&c_name);
            signature.apply(&f);
            f.set_functions(
                Some(aggregate_state_size_trampoline::<S>),
                Some(aggregate_init_trampoline::<S>),
                Some(aggregate_update_trampoline::<S>),
                Some(aggregate_combine_trampoline::<S>),
                Some(aggregate_finalize_trampoline::<S>),
            );
            f.set_destructor(Some(aggregate_destroy_trampoline::<S>));
            if S::special_handling() {
                f.set_special_handling();
            }
            set.add_function(&f)?;
        }
        set.register(self.raw_con(), name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{connection::Connection, types::value::DuckValue};

    /// A hand-written `VAggregate` computing a running sum, exercising the
    /// trait directly (the `#[duckdb_aggregate]` macro is a later phase).
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
        ) -> super::super::UdfResult<()> {
            let col = input.vector(0)?;
            let v: i32 = col.get(row)?;
            *state += i64::from(v);
            Ok(())
        }

        fn combine(
            source: &Self::State,
            target: &mut Self::State,
        ) -> super::super::UdfResult<()> {
            *target += *source;
            Ok(())
        }

        fn finalize(
            state: &Self::State,
            output: &mut VectorMut<'_>,
            row: usize,
        ) -> super::super::UdfResult<()> {
            output.set(row, *state)?;
            Ok(())
        }
    }

    #[test]
    fn register_and_call_matches_sql_sum() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.register_aggregate_function::<SumInts>("my_sum").unwrap();
        conn.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
        conn.execute_batch("INSERT INTO t VALUES (1), (2), (3), (41)").unwrap();
        let result = conn.execute("SELECT my_sum(v) AS r, sum(v) AS expected FROM t").unwrap();
        let rows: Vec<_> = result.collect::<Result<_>>().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("r"), Some(&DuckValue::BigInt(47)));
    }

    /// A `VAggregate` whose `update` always errors, verifying the error
    /// surfaces as a normal query error and the connection stays usable.
    struct AlwaysFails;

    impl VAggregate for AlwaysFails {
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
        ) -> super::super::UdfResult<()> {
            Err("deliberate update failure".into())
        }

        fn combine(
            _source: &Self::State,
            _target: &mut Self::State,
        ) -> super::super::UdfResult<()> {
            Ok(())
        }

        fn finalize(
            _state: &Self::State,
            output: &mut VectorMut<'_>,
            row: usize,
        ) -> super::super::UdfResult<()> {
            output.set(row, 0i32)?;
            Ok(())
        }
    }

    #[test]
    fn update_error_surfaces_as_query_error_and_connection_stays_usable() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.register_aggregate_function::<AlwaysFails>("always_fails").unwrap();
        conn.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
        conn.execute_batch("INSERT INTO t VALUES (1)").unwrap();
        let err = match conn.execute("SELECT always_fails(v) FROM t") {
            Ok(_) => panic!("expected an error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("deliberate update failure"), "{err}");
        // The connection must still be usable after a UDF error.
        conn.execute_batch("INSERT INTO t VALUES (2)").unwrap();
    }
}
