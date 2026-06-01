//! [`DuckDbConnection`] — a Diesel 2.2 `Connection` implementation for DuckDB.

use std::{marker::PhantomData, sync::Arc};

use better_duck_core::CachedStatement;
use diesel::{
    connection::{
        get_default_instrumentation, statement_cache::StatementCache, AnsiTransactionManager,
        ConnectionSealed, Instrumentation, InstrumentationEvent, LoadConnection, SimpleConnection,
        StrQueryHelper, TransactionManager,
    },
    expression::QueryMetadata,
    query_builder::{Query, QueryFragment, QueryId},
    result::{ConnectionError, ConnectionResult, QueryResult},
    Connection,
};

use crate::{
    backend::{DuckDb, DuckDbBindCollector},
    result::DuckDbError,
    row::Row,
};

// Connection struct

/// A Diesel connection to a DuckDB database.
///
/// Open with [`diesel::Connection::establish`], passing either `":memory:"` or a
/// file path (with an optional `"duckdb://"` prefix).
pub struct DuckDbConnection {
    pub(crate) inner: better_duck_core::connection::Connection,
    transaction_manager: AnsiTransactionManager,
    /// `get_default_instrumentation()` returns `Option<Box<dyn Instrumentation>>`.
    /// `Option` itself implements `Instrumentation`, so we store it directly.
    instrumentation: Option<Box<dyn Instrumentation>>,
    /// Prepared-statement cache — keyed by SQL text so each unique query is
    /// parsed and planned by DuckDB at most once per connection.
    statement_cache: StatementCache<DuckDb, CachedStatement>,
}

/// Strips an optional `"duckdb://"` URL scheme prefix.
fn parse_db_url(url: &str) -> &str {
    url.strip_prefix("duckdb://").unwrap_or(url)
}

// Diesel sealed trait

impl ConnectionSealed for DuckDbConnection {}

// SimpleConnection

impl SimpleConnection for DuckDbConnection {
    fn batch_execute(
        &mut self,
        query: &str,
    ) -> QueryResult<()> {
        self.instrumentation
            .on_connection_event(InstrumentationEvent::start_query(&StrQueryHelper::new(query)));
        let res: QueryResult<()> =
            self.inner.execute_batch(query).map_err(|e| DuckDbError::new(e).into());
        self.instrumentation.on_connection_event(InstrumentationEvent::finish_query(
            &StrQueryHelper::new(query),
            res.as_ref().err(),
        ));
        res
    }
}

// Connection

impl Connection for DuckDbConnection {
    type Backend = DuckDb;
    type TransactionManager = AnsiTransactionManager;

    fn establish(url: &str) -> ConnectionResult<Self> {
        let path = parse_db_url(url);
        let inner = if path == ":memory:" {
            better_duck_core::connection::Connection::open_in_memory()
        } else {
            better_duck_core::connection::Connection::open(path)
        }
        .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;

        Ok(DuckDbConnection {
            inner,
            transaction_manager: AnsiTransactionManager::default(),
            instrumentation: get_default_instrumentation(),
            statement_cache: StatementCache::new(),
        })
    }

    fn execute_returning_count<T>(
        &mut self,
        source: &T,
    ) -> QueryResult<usize>
    where
        T: QueryFragment<DuckDb> + QueryId,
    {
        // Collect bind values — source is only borrowed here.
        let mut bc = DuckDbBindCollector::default();
        source.collect_binds(&mut bc, &mut (), &DuckDb)?;

        // Split-field borrows: &self.inner (shared) + &mut self.statement_cache
        // + &mut self.instrumentation are three different struct fields.
        let inner = &self.inner;
        let mut stmt = self.statement_cache.cached_statement(
            source,
            &DuckDb,
            &[], // SQL text is the primary cache key; empty bind_types is sufficient.
            |sql, _| {
                CachedStatement::prepare(inner.db(), sql)
                    .map_err(|e| diesel::result::Error::from(DuckDbError::new(e)))
            },
            &mut self.instrumentation,
        )?;

        stmt.reset_bindings().map_err(|e| diesel::result::Error::from(DuckDbError::new(e)))?;
        for (i, bind) in bc.binds.iter_mut().enumerate() {
            stmt.bind((i + 1) as u64, bind)
                .map_err(|e| diesel::result::Error::from(DuckDbError::new(e)))?;
        }

        let mut res =
            stmt.execute().map_err(|e| diesel::result::Error::from(DuckDbError::new(e)))?;
        Ok(res.changes() as usize)
    }

    fn transaction_state(
        &mut self
    ) -> &mut <AnsiTransactionManager as TransactionManager<Self>>::TransactionStateData {
        &mut self.transaction_manager
    }

    fn instrumentation(&mut self) -> &mut dyn Instrumentation {
        &mut self.instrumentation
    }

    fn set_instrumentation(
        &mut self,
        i: impl Instrumentation,
    ) {
        self.instrumentation = Some(Box::new(i));
    }
}

// DuckDbCursor

/// Row iterator returned by [`LoadConnection::load`].
///
/// The lifetime parameter `'conn` ties the cursor to the mutable borrow of the
/// connection that produced it (required by Diesel's `LoadConnection` GAT).
pub struct DuckDbCursor<'conn> {
    result: better_duck_core::DuckResult,
    col_names: Arc<[Box<str>]>,
    _conn: PhantomData<&'conn mut DuckDbConnection>,
}

impl<'conn> DuckDbCursor<'conn> {
    fn new(result: better_duck_core::DuckResult) -> Self {
        let col_names: Arc<[Box<str>]> = result.column_names().to_vec().into();
        DuckDbCursor { result, col_names, _conn: PhantomData }
    }
}

impl<'conn> Iterator for DuckDbCursor<'conn> {
    type Item = QueryResult<Row<'conn>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.result.next().map(|r| {
            r.map(|row| Row::new(row, Arc::clone(&self.col_names)))
                .map_err(|e| diesel::result::Error::from(DuckDbError::new(e)))
        })
    }
}

// LoadConnection

impl LoadConnection for DuckDbConnection {
    type Cursor<'conn, 'query>
        = DuckDbCursor<'conn>
    where
        Self: 'conn;

    type Row<'conn, 'query>
        = Row<'conn>
    where
        Self: 'conn;

    fn load<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> QueryResult<DuckDbCursor<'conn>>
    where
        T: Query + QueryFragment<DuckDb> + QueryId + 'query,
        DuckDb: QueryMetadata<T::SqlType>,
    {
        let mut bc = DuckDbBindCollector::default();
        source.collect_binds(&mut bc, &mut (), &DuckDb)?;

        let inner = &self.inner;
        let mut stmt = self.statement_cache.cached_statement(
            &source,
            &DuckDb,
            &[],
            |sql, _| {
                CachedStatement::prepare(inner.db(), sql)
                    .map_err(|e| diesel::result::Error::from(DuckDbError::new(e)))
            },
            &mut self.instrumentation,
        )?;

        stmt.reset_bindings().map_err(|e| diesel::result::Error::from(DuckDbError::new(e)))?;
        for (i, bind) in bc.binds.iter_mut().enumerate() {
            stmt.bind((i + 1) as u64, bind)
                .map_err(|e| diesel::result::Error::from(DuckDbError::new(e)))?;
        }

        // stmt's cache borrow ends here (NLL: last use of stmt is execute()).
        let result =
            stmt.execute().map_err(|e| diesel::result::Error::from(DuckDbError::new(e)))?;
        Ok(DuckDbCursor::new(result))
    }
}

// Statement cache accessor

impl DuckDbConnection {
    /// Returns the number of prepared statements currently in the cache.
    ///
    /// Primarily useful for testing that cache hits are working correctly.
    pub fn statement_cache_len(&self) -> usize {
        self.statement_cache.len()
    }
}

// R2D2 connection pool

#[cfg(feature = "r2d2")]
impl diesel::r2d2::R2D2Connection for DuckDbConnection {
    /// Returns `Ok(())` if the connection can execute a trivial query.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection is broken or the database is unavailable.
    fn ping(&mut self) -> QueryResult<()> {
        self.batch_execute("SELECT 1")
    }

    /// Returns `true` if the internal transaction manager is in an error state.
    fn is_broken(&mut self) -> bool {
        AnsiTransactionManager::is_broken_transaction_manager(self)
    }
}

// Migration support

impl diesel::migration::MigrationConnection for DuckDbConnection {
    /// Creates the `__diesel_schema_migrations` tracking table if it does not
    /// already exist.
    ///
    /// Called automatically by `diesel migration run` before applying migrations.
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be created.
    fn setup(&mut self) -> QueryResult<usize> {
        self.batch_execute(
            "CREATE TABLE IF NOT EXISTS __diesel_schema_migrations (\
                version VARCHAR(50) PRIMARY KEY NOT NULL,\
                run_on  TIMESTAMP NOT NULL DEFAULT current_timestamp\
            )",
        )?;
        Ok(0)
    }
}
