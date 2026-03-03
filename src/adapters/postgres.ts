import PG, {
  Pool,
  PoolClient,
  QueryArrayConfig,
  QueryArrayResult,
  QueryConfig,
  QueryConfigValues,
  QueryResult,
  QueryResultRow,
  Submittable,
  escapeLiteral,
} from "pg";
import { TransactionException } from "@errors/index.js";
import { Logger } from "@utils/logger.js";

const types = PG.types;
types.setTypeParser(types.builtins.INT8, (x) => {
  const n = Number(x);
  return Number.isSafeInteger(n) ? n : x;
});
[types.builtins.NUMERIC, types.builtins.FLOAT4, types.builtins.FLOAT8].forEach(
  (id) => types.setTypeParser(id, parseFloat),
);
types.setTypeParser(types.builtins.BOOL, (val) => val === "t");

// --- Helper for Query Building ---
function prepareQuery(
  sql: string | QueryConfig,
  values?: any[],
  timeout?: number,
): QueryConfig {
  let config: QueryConfig =
    typeof sql === "string" ? { text: sql, values } : { ...sql };

  // Convert '?' syntax to Postgres '$1, $2' syntax
  if (config.text.includes("?")) {
    let index = 1;
    config.text = config.text.replace(/\?/g, () => `$${index++}`);
  }

  if (timeout) (config as any).query_timeout = timeout;
  return config;
}

/**
 * Handle for a single transaction.
 * This is "Concurrency Safe" because state is unique to this instance.
 */
export class Transaction {
  readonly __type = "transaction";
  private savepointCount = 0;

  constructor(
    private readonly client: PoolClient,
    private readonly timeout: number,
    private readonly pool: Pool,
  ) {}

  query<T extends Submittable>(queryStream: T): T;
  query<R extends any[] = any[], I = any[]>(
    queryConfig: QueryArrayConfig<I>,
    values?: QueryConfigValues<I>,
  ): Promise<QueryArrayResult<R>>;
  query<R extends QueryResultRow = any, I = any>(
    queryConfig: QueryConfig<I>,
  ): Promise<QueryResult<R>>;
  query<R extends QueryResultRow = any, I = any[]>(
    queryTextOrConfig: string | QueryConfig<I>,
    values?: QueryConfigValues<I>,
  ): Promise<QueryResult<R>>;
  async query<T = any>(
    sql: string | QueryConfig,
    values?: any[],
  ): Promise<any> {
    return await this.client.query(prepareQuery(sql, values, this.timeout));
  }

  async begin() {
    await this.query("BEGIN ISOLATION LEVEL READ COMMITTED");
  }

  async commit() {
    await this.query("COMMIT");
  }

  async rollback() {
    await this.query("ROLLBACK");
  }

  async savepoint(): Promise<string> {
    const name = `sp_${++this.savepointCount}`;
    await this.query(`SAVEPOINT ${name}`);
    return name;
  }

  async transaction<T>(callback: (tx: this) => Promise<T>): Promise<T> {
    const spName = await this.savepoint();
    try {
      const result = await callback(this);
      await this.releaseSavepoint(spName);
      return result;
    } catch (error) {
      await this.rollbackTo(spName);
      throw error;
    }
  }

  async releaseSavepoint(name: string) {
    await this.query(`RELEASE SAVEPOINT ${name}`);
  }

  async rollbackTo(name: string) {
    await this.query(`ROLLBACK TO SAVEPOINT ${name}`);
  }

  getPool(): Pool {
    return this.pool;
  }

  public quote(value: any): string {
    return escapeLiteral(value);
  }

  async ping(): Promise<void> {
    await this.query("SELECT 1");
  }

  async disconnect(): Promise<void> {
    this.client.release();
  }
}

/**
 * Main Database Client
 */
export class PostgresClient {
  readonly __type = "postgres";
  private pool: Pool;
  private readonly queryTimeout: number;

  constructor(pool: Pool, queryTimeout = 30000) {
    this.pool = pool;
    this.queryTimeout = queryTimeout;
  }

  query<T extends Submittable>(queryStream: T): T;
  query<R extends any[] = any[], I = any[]>(
    queryConfig: QueryArrayConfig<I>,
    values?: QueryConfigValues<I>,
  ): Promise<QueryArrayResult<R>>;
  query<R extends QueryResultRow = any, I = any>(
    queryConfig: QueryConfig<I>,
  ): Promise<QueryResult<R>>;
  query<R extends QueryResultRow = any, I = any[]>(
    queryTextOrConfig: string | QueryConfig<I>,
    values?: QueryConfigValues<I>,
  ): Promise<QueryResult<R>>;
  public async query(sql: string | QueryConfig, values?: any[]): Promise<any> {
    return await this.pool.query(prepareQuery(sql, values, this.queryTimeout));
  }

  public quote(value: any): string {
    return escapeLiteral(value);
  }

  async ping(): Promise<void> {
    await this.query("SELECT 1");
  }

  /**
   * Executes a callback in a safe, isolated transaction.
   * Handles retries for deadlocks and serialization failures.
   */
  async transaction<T>(
    callback: (tx: Transaction) => Promise<T>,
    maxRetries = 3,
  ): Promise<T> {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      const client = await this.pool.connect();
      const tx = new Transaction(client, this.queryTimeout, this.pool);

      try {
        await tx.begin();
        const result = await callback(tx);
        await tx.commit();
        return result;
      } catch (err: any) {
        await tx.rollback().catch((e) => Logger.error("Rollback error", e));

        const isRetryable = err.code === "40P01" || err.code === "40001";
        if (isRetryable && attempt < maxRetries) {
          const delay = Math.pow(2, attempt) * 50;
          await new Promise((res) => setTimeout(res, delay));
          continue;
        }
        throw err;
      } finally {
        client.release();
      }
    }
    throw new TransactionException("Transaction failed after max retries.");
  }

  async disconnect(): Promise<void> {
    await this.pool.end();
  }

  getPool(): Pool {
    return this.pool;
  }
}
