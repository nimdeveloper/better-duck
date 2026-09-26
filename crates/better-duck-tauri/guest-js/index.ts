import { invoke } from '@tauri-apps/api/core'

/** Result of a write/DDL statement. */
export interface ExecuteResult {
  /** Number of rows changed (0 for pure DDL). */
  rowsAffected: number
}

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
}
