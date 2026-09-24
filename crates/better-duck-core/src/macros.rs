//! Small declarative helper macros, exported at the crate root.
//!
//! These are `macro_rules!` (not procedural macros), so they carry no extra
//! dependency and work against any compatible connection type — notably both the
//! core [`Connection`](crate::connection::Connection) and the diesel
//! `DuckDbConnection`, which both expose `transaction(|c| -> Result<_>)`.

/// Runs a block inside a database transaction: commits if the block returns
/// `Ok`, rolls back if it returns `Err` (or panics).
///
/// The first argument is the connection *variable* (an identifier); it is
/// reborrowed and shadowed inside the block, so the block uses the same name.
/// Works with any connection exposing `transaction(|c| -> Result<_, _>)`.
///
/// ```
/// use better_duck_core::{connection::Connection, transaction};
///
/// # fn main() -> better_duck_core::error::Result<()> {
/// let mut conn = Connection::open_in_memory()?;
/// conn.execute_batch("CREATE TABLE t (id INTEGER)")?;
/// transaction!(conn, {
///     conn.execute_batch("INSERT INTO t VALUES (1)")?;
///     Ok(())
/// })?;
/// # Ok(())
/// # }
/// ```
#[macro_export]
macro_rules! transaction {
    ($conn:ident, $body:block) => {
        $conn.transaction(|$conn| $body)
    };
}

/// Builds a fixed-size array of `&mut dyn AppendAble` for
/// [`Connection::execute_with`](crate::connection::Connection::execute_with),
/// so heterogeneous bind values don't have to be spelled out by hand.
///
/// ```
/// use better_duck_core::{connection::Connection, params};
///
/// # fn main() -> better_duck_core::error::Result<()> {
/// let mut conn = Connection::open_in_memory()?;
/// conn.execute_batch("CREATE TABLE t (a INTEGER, b VARCHAR)")?;
/// conn.execute_with("INSERT INTO t VALUES ($1, $2)", &mut params![1_i32, "hi".to_owned()])?;
/// # Ok(())
/// # }
/// ```
#[macro_export]
macro_rules! params {
    ($($value:expr),* $(,)?) => {
        [$(&mut $value as &mut dyn $crate::types::appendable::AppendAble),*]
    };
}
