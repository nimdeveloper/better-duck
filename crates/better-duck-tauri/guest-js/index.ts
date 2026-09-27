import { invoke, Channel } from '@tauri-apps/api/core'

/** Result of a write/DDL statement. */
export interface ExecuteResult {
  /** Number of rows changed (0 for pure DDL). */
  rowsAffected: number
}

/** A columnar file format for import/export. */
export type DataFormat = 'parquet' | 'csv' | 'json'

/**
 * A handle to a loaded DuckDB database, mirroring the plugin's Rust commands.
 *
 * ```ts
 * const db = await Database.load('duckdb:analytics.db')
 * await db.execute('CREATE TABLE t (id INTEGER, name VARCHAR)')
 * const rows = await db.select('SELECT * FROM t WHERE id > $1', [10])
 * ```
 */
export default class Database {
  /** The connection string this handle was loaded with. */
  readonly path: string

  private constructor(path: string) {
    this.path = path
  }

  /** Open (and register) a database connection. */
  static async load(path: string): Promise<Database> {
    await invoke('plugin:duck|load', { db: path })
    return new Database(path)
  }

  /** Close this connection. Resolves to whether one was open. */
  async close(): Promise<boolean> {
    return await invoke('plugin:duck|close', { db: this.path })
  }

  /** Run a read query, returning rows as objects. */
  async select<T = Record<string, unknown>>(
    query: string,
    values: unknown[] = [],
  ): Promise<T[]> {
    return await invoke('plugin:duck|select', { db: this.path, query, values })
  }

  /** Run a write/DDL statement. */
  async execute(query: string, values: unknown[] = []): Promise<ExecuteResult> {
    return await invoke('plugin:duck|execute', { db: this.path, query, values })
  }

  /** Load (installing if needed) a DuckDB extension, e.g. `spatial`, `json`, `vss`. */
  async loadExtension(name: string): Promise<void> {
    return await invoke('plugin:duck|load_extension', { db: this.path, name })
  }

  /** Create `table` from a data file (path or glob). */
  async import(table: string, source: string, format: DataFormat): Promise<ExecuteResult> {
    return await invoke('plugin:duck|import', { db: this.path, table, source, format })
  }

  /** Create `table` from Parquet file(s). */
  async importParquet(table: string, source: string): Promise<ExecuteResult> {
    return this.import(table, source, 'parquet')
  }

  /** Create `table` from CSV file(s). */
  async importCsv(table: string, source: string): Promise<ExecuteResult> {
    return this.import(table, source, 'csv')
  }

  /** Create `table` from JSON file(s). */
  async importJson(table: string, source: string): Promise<ExecuteResult> {
    return this.import(table, source, 'json')
  }

  /** Export a query's result to a data file. */
  async export(query: string, path: string, format: DataFormat): Promise<ExecuteResult> {
    return await invoke('plugin:duck|export', { db: this.path, query, path, format })
  }

  /** Export a query's result to a Parquet file. */
  async exportParquet(query: string, path: string): Promise<ExecuteResult> {
    return this.export(query, path, 'parquet')
  }

  /** Export a query's result to a CSV file. */
  async exportCsv(query: string, path: string): Promise<ExecuteResult> {
    return this.export(query, path, 'csv')
  }

  /** Bulk-insert rows (objects keyed by column name) into a table. */
  async appendRows(table: string, rows: Record<string, unknown>[]): Promise<ExecuteResult> {
    return await invoke('plugin:duck|append_rows', { db: this.path, table, rows })
  }

  /** List the tables in the `main` schema. */
  async tables<T = Record<string, unknown>>(): Promise<T[]> {
    return await invoke('plugin:duck|tables', { db: this.path })
  }

  /** List a table's columns (name, type, nullability). */
  async columns<T = Record<string, unknown>>(table: string): Promise<T[]> {
    return await invoke('plugin:duck|columns', { db: this.path, table })
  }

  /** Return the query plan for a statement. */
  async explain<T = Record<string, unknown>>(query: string): Promise<T[]> {
    return await invoke('plugin:duck|explain', { db: this.path, query })
  }

  /**
   * Stream a query's rows to `onBatch` in chunks; resolves to the total row count.
   */
  async stream<T = Record<string, unknown>>(
    query: string,
    onBatch: (rows: T[]) => void,
    values: unknown[] = [],
    chunk?: number,
  ): Promise<number> {
    const channel = new Channel<T[]>()
    channel.onmessage = onBatch
    return await invoke('plugin:duck|stream', {
      db: this.path,
      query,
      values,
      chunk,
      channel,
    })
  }

  /** Flush the WAL into the main database file (FORCE CHECKPOINT when `force`). */
  async checkpoint(force = false): Promise<void> {
    return await invoke('plugin:duck|checkpoint', { db: this.path, force })
  }

  /** Cancel the in-flight streaming query on this connection; resolves to whether one was signalled. */
  async interrupt(): Promise<boolean> {
    return await invoke('plugin:duck|interrupt', { db: this.path })
  }
}
