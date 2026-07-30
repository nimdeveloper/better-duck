//! Ambient callback context for the `duck_projection!`/`duck_extra_info!`/
//! `duck_state!` ergonomics macros.
//!
//! The `#[duckdb_table_function]`/`#[duckdb_scalar]` attribute macros call the
//! user's plain function from *inside* their own generated `init`/`invoke`
//! trampolines — a different Rust function from the user's body, with no
//! parameter through which to reach `InitInfo`/the scalar `State`. These
//! guards stash a type-erased pointer in a thread-local for the exact
//! duration of that one call; the macros above read it back and clone the
//! value out (hence the `Clone` bound on `T`), so the returned value is
//! always independently owned — no reference to ambient context ever escapes,
//! which sidesteps the soundness problems a bare "conjure a reference from a
//! thread-local" API would otherwise have (nothing stops a caller from
//! stashing such a reference somewhere that outlives the callback).
//!
//! Calling one of the macros outside its matching callback panics with a
//! clear message. That panic is always contained by
//! [`contain_callback`](super::callback::contain_callback), so it surfaces as
//! a normal query error — never undefined behavior.

use std::cell::Cell;
use std::ffi::c_void;

thread_local! {
    static CURRENT_PROJECTION: Cell<*const Vec<usize>> = const { Cell::new(std::ptr::null()) };
    static CURRENT_TABLE_EXTRA_INFO: Cell<*const c_void> = const { Cell::new(std::ptr::null()) };
    static CURRENT_SCALAR_STATE: Cell<*const c_void> = const { Cell::new(std::ptr::null()) };
}

/// Sets the ambient projected-column-indices context for its lifetime,
/// restoring whatever was there before on drop.
pub struct ProjectionGuard(*const Vec<usize>);

impl ProjectionGuard {
    /// # Safety
    ///
    /// `indices` must remain valid (not moved, not dropped) for the entire
    /// lifetime of the returned guard.
    pub unsafe fn enter(indices: &Vec<usize>) -> Self {
        let prev = CURRENT_PROJECTION.with(|c| c.replace(indices as *const Vec<usize>));
        Self(prev)
    }
}

impl Drop for ProjectionGuard {
    fn drop(&mut self) {
        CURRENT_PROJECTION.with(|c| c.set(self.0));
    }
}

/// Sets the ambient table-function "extra info" context for its lifetime,
/// restoring whatever was there before on drop.
pub struct TableExtraInfoGuard(*const c_void);

impl TableExtraInfoGuard {
    /// # Safety
    ///
    /// `ptr`, if non-null, must remain valid for the entire lifetime of the
    /// returned guard, and must point to a value of whatever type
    /// `duck_extra_info!` is later instantiated with at the matching call site.
    pub unsafe fn enter(ptr: *const c_void) -> Self {
        let prev = CURRENT_TABLE_EXTRA_INFO.with(|c| c.replace(ptr));
        Self(prev)
    }
}

impl Drop for TableExtraInfoGuard {
    fn drop(&mut self) {
        CURRENT_TABLE_EXTRA_INFO.with(|c| c.set(self.0));
    }
}

/// Sets the ambient scalar-function `State` context for its lifetime,
/// restoring whatever was there before on drop.
pub struct ScalarStateGuard(*const c_void);

impl ScalarStateGuard {
    /// # Safety
    ///
    /// `ptr` must remain valid for the entire lifetime of the returned guard,
    /// and must point to a value of whatever type `duck_state!` is later
    /// instantiated with at the matching call site.
    pub unsafe fn enter(ptr: *const c_void) -> Self {
        let prev = CURRENT_SCALAR_STATE.with(|c| c.replace(ptr));
        Self(prev)
    }
}

impl Drop for ScalarStateGuard {
    fn drop(&mut self) {
        CURRENT_SCALAR_STATE.with(|c| c.set(self.0));
    }
}

/// Reads back the current projected-column-indices context, cloning it out.
///
/// # Panics
///
/// Panics if called outside a table function's `init` callback (contained by
/// `contain_callback`, so it surfaces as a normal query error).
#[doc(hidden)]
pub fn current_projection() -> Vec<usize> {
    CURRENT_PROJECTION.with(|c| {
        let ptr = c.get();
        assert!(
            !ptr.is_null(),
            "duck_projection!() called outside a table function's init callback"
        );
        // SAFETY: `ptr` is non-null, so it was set by `ProjectionGuard::enter`
        // and is still valid for the duration of the callback that guard
        // wraps — which strictly contains this call, since the guard is only
        // dropped after the user's plain fn (the only place this macro can
        // syntactically appear) returns.
        unsafe { (*ptr).clone() }
    })
}

/// Reads back the current table-function "extra info" context, cloning it out.
///
/// # Panics
///
/// Panics if called outside a table function callback that was registered
/// with extra info (contained by `contain_callback`).
#[doc(hidden)]
pub fn current_table_extra_info<T: Clone>() -> T {
    CURRENT_TABLE_EXTRA_INFO.with(|c| {
        let ptr = c.get();
        assert!(
            !ptr.is_null(),
            "duck_extra_info!() called outside a table function callback registered with extra info \
             (see `Connection::register_table_function_with_extra_info`)"
        );
        // SAFETY: see `current_projection` — same guard-lifetime argument.
        // The caller is responsible for requesting the same `T` that was
        // stored at registration time; there is no runtime type check.
        unsafe { (*ptr.cast::<T>()).clone() }
    })
}

/// Reads back the current scalar function `State` context, cloning it out.
///
/// # Panics
///
/// Panics if called outside a `#[duckdb_scalar]`-generated callback.
#[doc(hidden)]
pub fn current_scalar_state<T: Clone>() -> T {
    CURRENT_SCALAR_STATE.with(|c| {
        let ptr = c.get();
        assert!(!ptr.is_null(), "duck_state!() called outside a scalar function callback");
        // SAFETY: see `current_projection` — same guard-lifetime argument.
        // The caller is responsible for requesting the same `T` that the
        // enclosing `#[duckdb_scalar]` function declared as its state; there
        // is no runtime type check.
        unsafe { (*ptr.cast::<T>()).clone() }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "duck_projection!() called outside")]
    fn current_projection_panics_without_a_guard() {
        let _ = current_projection();
    }

    #[test]
    fn projection_guard_round_trips_and_restores_previous_value() {
        let outer = vec![1usize, 2, 3];
        // SAFETY: `outer` outlives both guards below.
        let outer_guard = unsafe { ProjectionGuard::enter(&outer) };
        assert_eq!(current_projection(), outer);

        {
            let inner = vec![9usize];
            // SAFETY: `inner` outlives this guard.
            let _inner_guard = unsafe { ProjectionGuard::enter(&inner) };
            assert_eq!(current_projection(), inner);
        }

        assert_eq!(
            current_projection(),
            outer,
            "dropping the inner guard must restore the outer one"
        );
        drop(outer_guard);
    }

    #[test]
    #[should_panic(expected = "duck_extra_info!() called outside")]
    fn current_table_extra_info_panics_without_a_guard() {
        let _: i32 = current_table_extra_info::<i32>();
    }

    #[test]
    fn table_extra_info_guard_round_trips() {
        let value = 42i32;
        // SAFETY: `value` outlives the guard.
        let _guard = unsafe {
            TableExtraInfoGuard::enter((&value as *const i32).cast::<std::ffi::c_void>())
        };
        assert_eq!(current_table_extra_info::<i32>(), 42);
    }

    #[test]
    #[should_panic(expected = "duck_state!() called outside")]
    fn current_scalar_state_panics_without_a_guard() {
        let _: i32 = current_scalar_state::<i32>();
    }

    #[test]
    fn scalar_state_guard_round_trips() {
        let value = 7i32;
        // SAFETY: `value` outlives the guard.
        let _guard =
            unsafe { ScalarStateGuard::enter((&value as *const i32).cast::<std::ffi::c_void>()) };
        assert_eq!(current_scalar_state::<i32>(), 7);
    }
}
