import { SQL } from "bun";
import { TransactionException } from "@errors/index.js";

/**
 * Converts '?' placeholders in SQL text to PostgreSQL-style '$1, $2, ...' syntax.
 * Avoids regex overhead in the hot path by using a direct char scan.
 */
function convertPlaceholders(text: string): string {
  if (text.indexOf("?") === -1) return text;

  let result = "";
  let paramIndex = 1;
  let lastPos = 0;

  for (let i = 0; i < text.length; i++) {
    if (text.charCodeAt(i) === 63 /* '?' */) {
      result += text.slice(lastPos, i) + "$" + paramIndex++;
      lastPos = i + 1;
    }
  }

  return lastPos === 0 ? text : result + text.slice(lastPos);
}

/** Minimal query result shape returned by our client layer. */
export interface QueryResult<R = any> {
  rows: R[];
  rowCount: number;
}

/** SQL literal escaping for values embedded directly in SQL strings. */
function escapeLiteral(value: any): string {
  if (value === null || value === undefined) return "NULL";
  if (typeof value === "number" || typeof value === "bigint")
    return String(value);
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  const str = String(value);
  return "'" + str.replace(/'/g, "''") + "'";
}

/**
 * Handle for a single transaction.
 * This is "Concurrency Safe" because state is unique to this instance.
 * Wraps a Bun SQL transaction handle (`tx` from `sql.begin()`).
 */
export class Transaction {
  readonly __type = "transaction" as const;
  private savepointCount = 0;

  constructor(
    private readonly bunTx: ReturnType<SQL["begin"]> extends Promise<infer U>
      ? any
      : any,
    private readonly timeout: number,
    private readonly sql: SQL,
    private readonly _database: string,
  ) {}

  async query<R = any>(
    sqlText: string | { text: string; values?: any[] },
    values?: any[],
  ): Promise<QueryResult<R>> {
    const text =
      typeof sqlText === "string"
        ? convertPlaceholders(sqlText)
        : convertPlaceholders(sqlText.text);
    const params =
      typeof sqlText === "string" ? values ?? [] : sqlText.values ?? [];

    const result = await this.bunTx.unsafe(text, params);
    return {
      rows: result as unknown as R[],
      rowCount: (result as any).count ?? (result as any).length ?? 0,
    };
  }

  async transaction<T>(callback: (tx: this) => Promise<T>): Promise<T> {
    return await this.bunTx.savepoint(async (sp: any) => {
      const nestedTx = new Transaction(
        sp,
        this.timeout,
        this.sql,
        this._database,
      ) as this;
      return await callback(nestedTx);
    });
  }

  getPool(): SQL {
    return this.sql;
  }

  get database(): string {
    return this._database;
  }

  public quote(value: any): string {
    return escapeLiteral(value);
  }

  async ping(): Promise<void> {
    await this.bunTx.unsafe("SELECT 1");
  }

  async disconnect(): Promise<void> {
    // Transaction handles are released when the transaction block completes.
    // No-op here to maintain API compatibility.
  }
}

/**
 * Main Database Client — wraps Bun's native SQL instance.
 * Replaces the previous `pg.Pool`-based implementation with Bun's built-in
 * binary protocol, connection pooling, and prepared statement caching.
 */
export class PostgresClient {
  readonly __type = "postgres" as const;
  private sql: SQL;
  private readonly queryTimeout: number;
  private readonly _database: string;

  constructor(sql: SQL | string, queryTimeout = 30000) {
    if (typeof sql === "string") {
      this.sql = new SQL(sql);
      // Extract database name from connection string
      try {
        const url = new URL(sql);
        this._database = url.pathname.slice(1) || "postgres";
      } catch {
        this._database = "postgres";
      }
    } else {
      this.sql = sql;
      // Bun SQL exposes `options.database` on the instance
      this._database = (sql as any).options?.database ?? "postgres";
    }
    this.queryTimeout = queryTimeout;
  }

  /**
   * Bridge method: accepts the old-style (sqlString, values[]) signature
   * and delegates to `sql.unsafe()` for execution via Bun's binary protocol.
   */
  public async query<R = any>(
    sqlText: string | { text: string; values?: any[] },
    values?: any[],
  ): Promise<QueryResult<R>> {
    const text =
      typeof sqlText === "string"
        ? convertPlaceholders(sqlText)
        : convertPlaceholders(sqlText.text);
    const params =
      typeof sqlText === "string" ? values ?? [] : sqlText.values ?? [];

    const result = await this.sql.unsafe(text, params);
    return {
      rows: result as unknown as R[],
      rowCount: (result as any).count ?? (result as any).length ?? 0,
    };
  }

  public quote(value: any): string {
    return escapeLiteral(value);
  }

  async ping(): Promise<void> {
    await this.sql`SELECT 1`;
  }

  /**
   * Executes a callback inside a transaction with automatic retry for
   * deadlocks (40P01) and serialization failures (40001).
   */
  async transaction<T>(
    callback: (tx: Transaction) => Promise<T>,
    maxRetries = 3,
  ): Promise<T> {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.sql.begin(async (bunTx) => {
          const tx = new Transaction(
            bunTx,
            this.queryTimeout,
            this.sql,
            this._database,
          );
          return await callback(tx);
        });
      } catch (err: any) {
        const isRetryable = err.code === "40P01" || err.code === "40001";
        if (isRetryable && attempt < maxRetries) {
          const delay = (1 << attempt) * 50; // 100ms, 200ms, 400ms…
          await Bun.sleep(delay);
          continue;
        }
        throw err;
      }
    }
    throw new TransactionException("Transaction failed after max retries.");
  }

  async disconnect(): Promise<void> {
    await this.sql.close();
  }

  getPool(): SQL {
    return this.sql;
  }

  get database(): string {
    return this._database;
  }
}
