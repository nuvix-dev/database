import { Database } from "bun:sqlite";
import { DatabaseException } from "@errors/base.js";
import type { QueryClient, TransactionClient } from "./interface.js";
import { processSQLiteException } from "./sqlite-error-mapper.js";
import {
  bindSQLiteValues,
  type SQLiteBoundValue,
} from "./sqlite-values.js";
import type { DatabaseError, QueryResult } from "./types.js";

export type { DatabaseError, QueryResult } from "./types.js";

export type SQLiteConfig = string | Database;

function execute<T>(
  database: Database,
  sql: string,
  values?: readonly unknown[],
): QueryResult<T> {
  try {
    const statement = database.query<T, SQLiteBoundValue[]>(sql);
    const bindings = bindSQLiteValues(values) ?? [];

    if (statement.columnNames.length > 0) {
      const rows = statement.all(...bindings);
      return { rows, rowCount: rows.length };
    }

    const result = statement.run(...bindings);
    return { rows: [], rowCount: result.changes };
  } catch (error) {
    processSQLiteException(error);
  }
}

function identifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`;
}

/** Escapes a SQLite string literal for safe direct SQL inclusion. */
export function escapeSQLiteLiteral(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

/** Query handle bound to the currently active transaction. */
export class SQLiteTransaction implements TransactionClient {
  readonly __type = "transaction";
  private active = true;
  private savepointCount = 0;

  constructor(
    private readonly handle: Database,
    private readonly owner: SQLiteClient,
  ) {}

  async query<T = Record<string, unknown>>(
    sql: string,
    values?: unknown[],
  ): Promise<QueryResult<T>> {
    this.assertActive();
    return execute<T>(this.handle, sql, values);
  }

  async transaction<T>(
    callback: (client: this) => Promise<T>,
    _maxRetries?: number,
  ): Promise<T> {
    const name = await this.savepoint();

    try {
      const result = await callback(this);
      await this.releaseSavepoint(name);
      return result;
    } catch (error) {
      await this.rollbackTo(name);
      await this.releaseSavepoint(name);
      throw error;
    }
  }

  async savepoint(): Promise<string> {
    this.assertActive();
    const name = `nuvix_sp_${++this.savepointCount}`;
    await this.query(`SAVEPOINT ${identifier(name)}`);
    return name;
  }

  async releaseSavepoint(name: string): Promise<void> {
    this.assertActive();
    await this.query(`RELEASE SAVEPOINT ${identifier(name)}`);
  }

  async rollbackTo(name: string): Promise<void> {
    this.assertActive();
    await this.query(`ROLLBACK TO SAVEPOINT ${identifier(name)}`);
  }

  get database(): string {
    return this.owner.database;
  }

  async disconnect(): Promise<void> {}

  quote(value: string): string {
    return escapeSQLiteLiteral(value);
  }

  async ping(): Promise<void> {
    await this.query("SELECT 1");
  }

  deactivate(): void {
    this.active = false;
  }

  private assertActive(): void {
    if (!this.active) {
      throw new DatabaseException("SQLite transaction is no longer active");
    }
  }
}

/** Bun-native SQLite query client with async-safe transaction ownership. */
export class SQLiteClient implements QueryClient {
  readonly __type = "sqlite";
  private readonly handle: Database;
  private readonly ownsHandle: boolean;
  private queue: Promise<void> = Promise.resolve();
  private closed = false;

  constructor(config: SQLiteConfig) {
    this.ownsHandle = typeof config === "string";
    this.handle =
      typeof config === "string"
        ? new Database(config, { strict: true })
        : config;
  }

  async query<T = Record<string, unknown>>(
    sql: string,
    values?: unknown[],
  ): Promise<QueryResult<T>> {
    return this.enqueue(() => {
      this.assertOpen();
      return execute<T>(this.handle, sql, values);
    });
  }

  /**
   * Holds exclusive access to the handle until the async callback settles.
   * Bun's transaction helper is synchronous and would commit at the first await.
   */
  async transaction<T>(
    callback: (client: SQLiteTransaction) => Promise<T>,
    _maxRetries?: number,
  ): Promise<T> {
    return this.enqueue(async () => {
      this.assertOpen();
      execute(this.handle, "BEGIN");
      const transaction = new SQLiteTransaction(this.handle, this);

      try {
        const result = await callback(transaction);
        execute(this.handle, "COMMIT");
        return result;
      } catch (error) {
        if (this.handle.inTransaction) execute(this.handle, "ROLLBACK");
        throw error;
      } finally {
        transaction.deactivate();
      }
    });
  }

  get database(): string {
    return this.handle.filename;
  }

  quote(value: string): string {
    return escapeSQLiteLiteral(value);
  }

  async ping(): Promise<void> {
    await this.query("SELECT 1");
  }

  async disconnect(): Promise<void> {
    await this.enqueue(() => {
      if (this.closed) return;
      this.closed = true;
      if (this.ownsHandle) this.handle.close(true);
    });
  }

  private assertOpen(): void {
    if (this.closed) throw new DatabaseException("SQLite client is disconnected");
  }

  private enqueue<T>(operation: () => T | Promise<T>): Promise<T> {
    const result = this.queue.then(operation, operation);
    this.queue = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }
}
