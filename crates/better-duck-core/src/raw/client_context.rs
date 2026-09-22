//! RAII wrapper for `duckdb_client_context`.
//!
//! A client context is obtained from a connection (or from a table function's bind
//! info) and exposes the stable connection id. The handle is owned — it must be
//! destroyed with `duckdb_destroy_client_context` — but it is only meaningful while
//! the connection (or bind call) it came from is alive. [`ClientContext`] is
//! therefore **lifetime-bound** to that owner: the borrow it captures prevents it
//! from outliving the connection at compile time, and `Drop` destroys the handle
//! exactly once.
// FFI pointer args are used safely inside `unsafe` blocks.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::marker::PhantomData;

use crate::ffi::{
    duckdb_client_context, duckdb_client_context_get_connection_id, duckdb_destroy_client_context,
};

/// An owned `duckdb_client_context`, borrow-tied to the owner it came from so it
/// cannot outlive it.
pub struct ClientContext<'owner> {
    ctx: duckdb_client_context,
    _owner: PhantomData<&'owner ()>,
}

impl<'owner> ClientContext<'owner> {
    /// Wraps a raw client-context handle, or `None` if null.
    ///
    /// # Safety
    ///
    /// `ctx` must be a `duckdb_client_context` produced for an owner that outlives
    /// `'owner` (a connection or a live bind call), and ownership of the handle is
    /// transferred here (destroyed on drop).
    pub(crate) unsafe fn from_raw(ctx: duckdb_client_context) -> Option<ClientContext<'owner>> {
        if ctx.is_null() {
            return None;
        }
        Some(ClientContext { ctx, _owner: PhantomData })
    }

    /// The stable connection id of the connection this context belongs to.
    #[must_use]
    pub fn connection_id(&self) -> u64 {
        // SAFETY: `self.ctx` is a valid, non-null client context owned by `self`.
        unsafe { duckdb_client_context_get_connection_id(self.ctx) as u64 }
    }
}

impl Drop for ClientContext<'_> {
    fn drop(&mut self) {
        if !self.ctx.is_null() {
            // SAFETY: `self.ctx` is a valid, non-null client context created by one of
            // the get-context functions and not yet destroyed; destroyed once here.
            unsafe { duckdb_destroy_client_context(&mut self.ctx) };
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::connection::Connection;

    #[test]
    fn connection_exposes_a_stable_client_context_id() {
        let conn = Connection::open_in_memory().unwrap();
        let id = conn.client_context().expect("client context").connection_id();
        // The same connection reports the same id on a second fetch.
        assert_eq!(conn.client_context().unwrap().connection_id(), id);
    }

    #[test]
    fn distinct_connections_on_one_db_have_distinct_ids() {
        let a = Connection::open_in_memory().unwrap();
        let b = a.try_clone().expect("second connection on the same database");
        let ida = a.client_context().unwrap().connection_id();
        let idb = b.client_context().unwrap().connection_id();
        assert_ne!(ida, idb, "each connection has its own id");
    }
}
