//! DuckDB aggregate functions: functions that fold many rows into one value per
//! group, e.g. `SELECT my_sum(x) FROM t GROUP BY k`.
//!
//! An aggregate is defined by a per-group [`State`](VAggregate::State) and four
//! callbacks — init, update, combine, finalize — plus a destructor. DuckDB allocates
//! the raw state bytes uninitialised, so the state is wrapped in a [`RawState`] that
//! pairs the value with an `initialised` flag (a `MaybeUninit` guard), and every
//! callback that runs user code is wrapped in [`contain_callback`] so a panic can
//! never cross the C boundary.

mod function;

use std::ffi::CString;
use std::mem::MaybeUninit;
use std::panic::{catch_unwind, AssertUnwindSafe};

use crate::{
    connection::Connection,
    error::Result,
    ffi::{duckdb_aggregate_state, duckdb_data_chunk, duckdb_function_info, duckdb_vector, idx_t},
};

use self::function::{AggregateFunction, AggregateFunctionInfo};
use super::{callback::contain_callback, data_chunk::DataChunkHandle, vector::VectorMut};
use crate::types::LogicalType;

/// A DuckDB aggregate function: folds a set of input rows into one result value.
///
/// See the callback-containment contract in [`crate::udf`].
pub trait VAggregate: Sized {
    /// The per-group accumulator. Allocated by DuckDB (uninitialised), set up by
    /// [`init`](VAggregate::init), and dropped by the generated destructor.
    type State: Send + Sync + 'static;

    /// Registration-time shared state, available read-only to every callback via
    /// DuckDB's extra-info slot. Use `()` if the aggregate needs none.
    type Shared: Send + Sync + 'static;

    /// The input parameter types (one overload).
    ///
    /// # Errors
    ///
    /// Returns an error if a parameter's logical type cannot be built.
    fn parameters() -> Result<Vec<LogicalType>>;

    /// The result type produced by [`finalize`](VAggregate::finalize).
    ///
    /// # Errors
    ///
    /// Returns an error if the return type cannot be built.
    fn return_type() -> Result<LogicalType>;

    /// The initial accumulator value for a fresh group.
    fn init() -> Self::State;

    /// Folds `input`'s row `row` into `state`. `shared` is the registration state.
    ///
    /// # Errors
    ///
    /// Returns an error to fail the query with that message.
    fn update(
        shared: &Self::Shared,
        state: &mut Self::State,
        input: &DataChunkHandle,
        row: usize,
    ) -> super::UdfResult<()>;

    /// Merges `source` into `target` (parallel/partial aggregation).
    fn combine(
        shared: &Self::Shared,
        source: &Self::State,
        target: &mut Self::State,
    );

    /// Writes the finalised value of `state` into `output` at `row`.
    ///
    /// # Errors
    ///
    /// Returns an error to fail the query with that message.
    fn finalize(
        shared: &Self::Shared,
        state: &Self::State,
        output: &mut VectorMut<'_>,
        row: usize,
    ) -> super::UdfResult<()>;

    /// Whether the aggregate should still be invoked for `NULL` inputs. `false` by
    /// default (DuckDB skips rows whose argument is `NULL`).
    fn special_handling() -> bool {
        false
    }
}

/// DuckDB-allocated aggregate state: the accumulator plus an `initialised` flag, so
/// the (uninitialised) bytes DuckDB hands out are only read after `init` runs and
/// dropped at most once.
struct RawState<S> {
    initialised: bool,
    data: MaybeUninit<S>,
}

impl<S> RawState<S> {
    fn ready(value: S) -> Self {
        Self { initialised: true, data: MaybeUninit::new(value) }
    }

    fn poisoned() -> Self {
        Self { initialised: false, data: MaybeUninit::uninit() }
    }

    /// # Safety
    /// The state must have been initialised by `init`.
    unsafe fn get(&self) -> &S {
        debug_assert!(self.initialised, "aggregate state read before init");
        // SAFETY: `initialised` implies `data` holds a valid `S` (caller contract).
        unsafe { &*self.data.as_ptr() }
    }

    /// # Safety
    /// The state must have been initialised by `init`.
    unsafe fn get_mut(&mut self) -> &mut S {
        debug_assert!(self.initialised, "aggregate state written before init");
        // SAFETY: `initialised` implies `data` holds a valid `S` (caller contract).
        unsafe { &mut *self.data.as_mut_ptr() }
    }

    /// Drops the contained value if present, leaving the slot poisoned. Idempotent.
    unsafe fn drop_value(&mut self) {
        if self.initialised {
            self.initialised = false;
            // SAFETY: `initialised` was true, so `data` held a valid `S`.
            unsafe { self.data.assume_init_drop() };
        }
    }
}

/// `state_size` callback: the byte size of one wrapped state.
unsafe extern "C" fn agg_state_size<A: VAggregate>(_info: duckdb_function_info) -> idx_t {
    std::mem::size_of::<RawState<A::State>>() as idx_t
}

/// `init` callback: write the initial wrapped state into DuckDB's raw bytes.
unsafe extern "C" fn agg_init<A: VAggregate>(
    _info: duckdb_function_info,
    state: duckdb_aggregate_state,
) {
    let slot = state as *mut RawState<A::State>;
    // A user `init` panic must not cross the C boundary; on panic leave the slot
    // poisoned so the destructor is still safe.
    let value = catch_unwind(AssertUnwindSafe(A::init));
    // SAFETY: `slot` points at `size_of::<RawState<A::State>>()` uninitialised bytes
    // DuckDB just allocated; writing a fresh `RawState` initialises them exactly once.
    unsafe {
        match value {
            Ok(v) => slot.write(RawState::ready(v)),
            Err(_) => slot.write(RawState::poisoned()),
        }
    }
}

/// `update` callback: fold each input row into its group's state.
unsafe extern "C" fn agg_update<A: VAggregate>(
    info: duckdb_function_info,
    input: duckdb_data_chunk,
    states: *mut duckdb_aggregate_state,
) {
    let sink = AggregateFunctionInfo::from(info);
    contain_callback(&sink, || {
        // SAFETY: the extra-info stored at registration has type `A::Shared`.
        let shared = unsafe { sink.extra_info::<A::Shared>() };
        // SAFETY: DuckDB owns `input` for the call and guarantees it stays live.
        let chunk = unsafe { DataChunkHandle::borrowed(input) };
        for row in 0..chunk.len() {
            // SAFETY: `states` has one entry per input row; `states[row]` points at a
            // live, init'd `RawState<A::State>` for that row's group.
            let slot = unsafe { &mut *((*states.add(row)) as *mut RawState<A::State>) };
            // SAFETY: the slot was initialised by `agg_init` before any update.
            let state = unsafe { slot.get_mut() };
            A::update(shared, state, &chunk, row)?;
        }
        Ok(())
    });
}

/// `combine` callback: merge each source state into the matching target state.
unsafe extern "C" fn agg_combine<A: VAggregate>(
    info: duckdb_function_info,
    source: *mut duckdb_aggregate_state,
    target: *mut duckdb_aggregate_state,
    count: idx_t,
) {
    let sink = AggregateFunctionInfo::from(info);
    contain_callback(&sink, || {
        // SAFETY: the extra-info stored at registration has type `A::Shared`.
        let shared = unsafe { sink.extra_info::<A::Shared>() };
        for i in 0..count as usize {
            // SAFETY: `source[i]` points at a live, init'd `RawState<A::State>`.
            let src = unsafe { &*((*source.add(i)) as *const RawState<A::State>) };
            // SAFETY: `target[i]` points at a live, init'd `RawState<A::State>`.
            let tgt = unsafe { &mut *((*target.add(i)) as *mut RawState<A::State>) };
            // SAFETY: both slots were initialised by `agg_init` before any combine.
            let (s, t) = unsafe { (src.get(), tgt.get_mut()) };
            A::combine(shared, s, t);
        }
        Ok(())
    });
}

/// `finalize` callback: write each state's finalised value into the result vector.
unsafe extern "C" fn agg_finalize<A: VAggregate>(
    info: duckdb_function_info,
    source: *mut duckdb_aggregate_state,
    result: duckdb_vector,
    count: idx_t,
    offset: idx_t,
) {
    let sink = AggregateFunctionInfo::from(info);
    contain_callback(&sink, || {
        // SAFETY: the extra-info stored at registration has type `A::Shared`.
        let shared = unsafe { sink.extra_info::<A::Shared>() };
        // SAFETY: DuckDB owns `result` for the call and guarantees it stays live.
        let mut out = unsafe { VectorMut::new(result) };
        for i in 0..count as usize {
            // SAFETY: `source[i]` points at a live, init'd `RawState<A::State>`.
            let slot = unsafe { &*((*source.add(i)) as *const RawState<A::State>) };
            // SAFETY: the slot was initialised by `agg_init` before finalize.
            let state = unsafe { slot.get() };
            A::finalize(shared, state, &mut out, offset as usize + i)?;
        }
        Ok(())
    });
}

/// `destroy` callback: drop each state's value exactly once.
unsafe extern "C" fn agg_destroy<A: VAggregate>(
    states: *mut duckdb_aggregate_state,
    count: idx_t,
) {
    // A `Drop` panic must not cross the C boundary.
    let _ = catch_unwind(AssertUnwindSafe(|| {
        for i in 0..count as usize {
            // SAFETY: `states[i]` points at a live `RawState<A::State>` DuckDB is
            // destroying; `drop_value` drops the contained value at most once.
            let slot = unsafe { &mut *((*states.add(i)) as *mut RawState<A::State>) };
            // SAFETY: as above.
            unsafe { slot.drop_value() };
        }
    }));
}

impl Connection {
    /// Registers `A` as an aggregate function named `name`, with a default shared
    /// state.
    ///
    /// # Errors
    ///
    /// Returns an error if `name` contains a NUL byte, a logical type cannot be
    /// built, or DuckDB rejects the registration (e.g. a name conflict).
    pub fn register_aggregate_function<A: VAggregate>(
        &mut self,
        name: &str,
    ) -> Result<()>
    where
        A::Shared: Default,
    {
        self.register_aggregate_function_with_state::<A>(name, A::Shared::default())
    }

    /// Registers `A` as an aggregate function named `name`, with an explicit shared
    /// registration state (available read-only to every callback).
    ///
    /// # Errors
    ///
    /// As [`register_aggregate_function`](Connection::register_aggregate_function).
    pub fn register_aggregate_function_with_state<A: VAggregate>(
        &mut self,
        name: &str,
        shared: A::Shared,
    ) -> Result<()> {
        let c_name = CString::new(name)?;
        let f = AggregateFunction::new(&c_name);
        for p in A::parameters()? {
            f.add_parameter(&p);
        }
        f.set_return_type(&A::return_type()?);
        f.set_extra_info(shared);
        f.set_functions(
            Some(agg_state_size::<A>),
            Some(agg_init::<A>),
            Some(agg_update::<A>),
            Some(agg_combine::<A>),
            Some(agg_finalize::<A>),
        );
        f.set_destructor(Some(agg_destroy::<A>));
        if A::special_handling() {
            f.set_special_handling();
        }
        f.register(self.raw_con(), name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::value::DuckValue;

    /// `int_sum(x)`: a BIGINT running sum over INTEGER inputs.
    struct IntSum;

    impl VAggregate for IntSum {
        type State = i64;
        type Shared = ();

        fn parameters() -> Result<Vec<LogicalType>> {
            Ok(vec![LogicalType::of::<i32>()?])
        }

        fn return_type() -> Result<LogicalType> {
            LogicalType::of::<i64>()
        }

        fn init() -> i64 {
            0
        }

        fn update(
            _shared: &(),
            state: &mut i64,
            input: &DataChunkHandle,
            row: usize,
        ) -> super::super::UdfResult<()> {
            let v: i32 = input.vector(0)?.get(row)?;
            *state += i64::from(v);
            Ok(())
        }

        fn combine(
            _shared: &(),
            source: &i64,
            target: &mut i64,
        ) {
            *target += *source;
        }

        fn finalize(
            _shared: &(),
            state: &i64,
            output: &mut VectorMut<'_>,
            row: usize,
        ) -> super::super::UdfResult<()> {
            output.set(row, *state)?;
            Ok(())
        }
    }

    #[test]
    fn aggregate_sums_and_groups() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.register_aggregate_function::<IntSum>("int_sum").unwrap();
        conn.execute_batch("CREATE TABLE t (k INTEGER, v INTEGER)").unwrap();
        conn.execute_batch("INSERT INTO t VALUES (1,10),(1,20),(2,5),(2,7),(2,100)").unwrap();

        // Grand total.
        let total = conn.execute("SELECT int_sum(v) AS s FROM t").unwrap();
        let rows: Vec<_> = total.collect::<Result<_>>().unwrap();
        assert_eq!(rows[0].get("s"), Some(&DuckValue::BigInt(142)));

        // Grouped — exercises combine across partial states too.
        let mut grouped =
            conn.execute("SELECT k, int_sum(v) AS s FROM t GROUP BY k ORDER BY k").unwrap();
        let g0 = grouped.next().unwrap().unwrap();
        assert_eq!(g0.get("s"), Some(&DuckValue::BigInt(30)));
        let g1 = grouped.next().unwrap().unwrap();
        assert_eq!(g1.get("s"), Some(&DuckValue::BigInt(112)));
    }
}
