//! DuckDB custom cast functions: register a conversion from one logical type to
//! another, used implicitly by the binder or explicitly via `CAST`.
//!
//! A cast is defined by its source/target types, an implicit-cast cost (how eagerly
//! the binder applies it), and a per-row conversion. DuckDB runs the conversion in
//! two *modes*: a normal `CAST` (a conversion error fails the query) and a `TRY_CAST`
//! (a conversion error yields `NULL` for that row instead). [`VCast::cast_row`]
//! reports a row error and the trampoline routes it per the active mode.
// FFI pointer args are used safely inside `unsafe` blocks.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

mod function;

use crate::{
    connection::Connection,
    error::Result,
    ffi::{duckdb_cast_mode_DUCKDB_CAST_TRY, duckdb_function_info, duckdb_vector, idx_t},
};

use self::function::{CastFunction, CastFunctionInfo};
use super::{vector::VectorMut, vector::VectorRef};
use crate::types::LogicalType;

/// A DuckDB custom cast: converts values of a source logical type to a target type.
///
/// See the callback-containment contract in [`crate::udf`].
pub trait VCast: Sized {
    /// Registration-time shared state, available read-only to the cast callback.
    /// Use `()` if the cast needs none.
    type Shared: Send + Sync + 'static;

    /// The source (input) logical type.
    ///
    /// # Errors
    /// Returns an error if the type cannot be built.
    fn source_type() -> Result<LogicalType>;

    /// The target (output) logical type.
    ///
    /// # Errors
    /// Returns an error if the type cannot be built.
    fn target_type() -> Result<LogicalType>;

    /// The implicit-cast cost the binder uses to choose this cast (lower = preferred;
    /// a negative value disables implicit application, requiring an explicit `CAST`).
    /// Defaults to `-1` (explicit only).
    fn implicit_cast_cost() -> i64 {
        -1
    }

    /// Converts `input[row]` and writes it to `output[row]`.
    ///
    /// # Errors
    ///
    /// Returns an error for a value that cannot be converted. Under a normal `CAST`
    /// the query fails with that message; under `TRY_CAST` the output row is set to
    /// `NULL` instead.
    fn cast_row(
        shared: &Self::Shared,
        input: &VectorRef<'_>,
        output: &mut VectorMut<'_>,
        row: usize,
    ) -> super::UdfResult<()>;
}

/// The C trampoline installed via `duckdb_cast_function_set_function`.
///
/// Returns `true` on full success. On a per-row conversion error it consults the
/// active cast mode: `TRY` sets that output row to `NULL` and continues; a normal
/// cast reports the error and returns `false`. A panic is contained the same way as
/// a hard error.
unsafe extern "C" fn cast_trampoline<C: VCast>(
    info: duckdb_function_info,
    count: idx_t,
    input: duckdb_vector,
    output: duckdb_vector,
) -> bool {
    let sink = CastFunctionInfo::from(info);
    // SAFETY: the extra-info stored at registration has type `C::Shared`.
    let shared = unsafe { sink.extra_info::<C::Shared>() };
    // SAFETY: DuckDB owns `input`/`output` for the call and guarantees they stay live.
    let in_vec = unsafe { VectorRef::new(input) };
    let is_try = sink.cast_mode() == duckdb_cast_mode_DUCKDB_CAST_TRY;

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: `output` is a valid, live output vector owned by DuckDB for the call.
        let mut out = unsafe { VectorMut::new(output) };
        for row in 0..count as usize {
            if let Err(e) = C::cast_row(shared, &in_vec, &mut out, row) {
                if is_try {
                    // TRY_CAST: this row becomes NULL; keep converting the rest.
                    sink.set_row_error(&e.to_string(), row as idx_t, output);
                } else {
                    // Normal CAST: fail the whole cast with this message.
                    sink.set_error(&e.to_string());
                    return false;
                }
            }
        }
        true
    }));

    match result {
        Ok(ok) => ok,
        Err(_) => {
            sink.set_error("cast function panicked");
            false
        },
    }
}

impl Connection {
    /// Registers `C` as a custom cast, with a default shared state.
    ///
    /// # Errors
    ///
    /// Returns an error if a logical type cannot be built or DuckDB rejects the
    /// registration.
    pub fn register_cast_function<C: VCast>(&mut self) -> Result<()>
    where
        C::Shared: Default,
    {
        self.register_cast_function_with_state::<C>(C::Shared::default())
    }

    /// Registers `C` as a custom cast, with an explicit shared state.
    ///
    /// # Errors
    ///
    /// As [`register_cast_function`](Connection::register_cast_function).
    pub fn register_cast_function_with_state<C: VCast>(
        &mut self,
        shared: C::Shared,
    ) -> Result<()> {
        let f = CastFunction::new();
        f.set_source_type(&C::source_type()?);
        f.set_target_type(&C::target_type()?);
        f.set_implicit_cast_cost(C::implicit_cast_cost());
        f.set_extra_info(shared);
        f.set_function(Some(cast_trampoline::<C>));
        f.register(self.raw_con())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::value::DuckValue;

    /// Casts VARCHAR → INTEGER by parsing; an unparseable value is a row error
    /// (NULL under TRY_CAST, a query failure under a normal CAST).
    struct StrToInt;

    impl VCast for StrToInt {
        type Shared = ();

        fn source_type() -> Result<LogicalType> {
            LogicalType::of::<String>()
        }

        fn target_type() -> Result<LogicalType> {
            LogicalType::of::<i32>()
        }

        fn implicit_cast_cost() -> i64 {
            -1
        }

        fn cast_row(
            _shared: &(),
            input: &VectorRef<'_>,
            output: &mut VectorMut<'_>,
            row: usize,
        ) -> super::super::UdfResult<()> {
            let s: &str = input.get(row)?;
            let v: i32 = s.trim().parse().map_err(|_| format!("not an integer: {s:?}"))?;
            output.set(row, v)?;
            Ok(())
        }
    }

    #[test]
    fn custom_cast_converts_and_try_cast_nulls_bad_rows() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.register_cast_function::<StrToInt>().unwrap();

        // Explicit CAST of a good value.
        let mut ok = conn.execute("SELECT CAST('42' AS INTEGER) AS n").unwrap();
        assert_eq!(ok.next().unwrap().unwrap().get("n"), Some(&DuckValue::Int(42)));

        // TRY_CAST of a bad value yields NULL (row error path).
        let mut bad = conn.execute("SELECT TRY_CAST('oops' AS INTEGER) AS n").unwrap();
        assert_eq!(bad.next().unwrap().unwrap().get("n"), Some(&DuckValue::Null));

        // A normal CAST of a bad value fails the query (whole-cast error path), and
        // the connection stays usable afterwards.
        assert!(conn.execute("SELECT CAST('oops' AS INTEGER) AS n").is_err());
        let _ = conn.execute("SELECT 1").unwrap();
    }
}
