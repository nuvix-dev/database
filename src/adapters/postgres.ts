import { SQL } from "bun";
import { TransactionException } from "@errors/index.js";
import { Logger } from "@utils/logger.js";
import { JsonParam } from "./types.js";
import type { QueryClient, TransactionClient } from "./interface.js";
import type { DatabaseError, QueryResult } from "./types.js";

export type { DatabaseError, QueryResult } from "./types.js";

function isSqlClient(value: unknown): value is SQL {
  if (
    (typeof value !== "object" || value === null) &&
    typeof value !== "function"
  ) {
    return false;
  }

  return (
    "unsafe" in value &&
    typeof value.unsafe === "function" &&
    "begin" in value &&
    typeof value.begin === "function" &&
    "close" in value &&
    typeof value.close === "function"
  );
}

/**
 * Converts `?` placeholders (used throughout the SQL builders) into
 * PostgreSQL `$1..$n` positional parameters expected by Bun.sql.
 *
 * Lexer-aware: skips `?` characters that appear inside string literals,
 * quoted identifiers, comments, or dollar-quoted bodies so they are never
 * mistaken for bind placeholders.
 */
export function toPositionalParams(text: string): string {
  if (!text.includes("?")) return text;
  let index = 1;
  let out = "";
  let i = 0;
  const n = text.length;

  const isDollarTagStart = (pos: number): string | null => {
    if (text[pos] !== "$") return null;
    // Matches both tagged ($tag$) and empty ($$) dollar-quote delimiters.
    const match = /^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/.exec(text.slice(pos));
    return match ? match[0] : null;
  };

  while (i < n) {
    const c = text[i];

    // Line comment: skip to end of line.
    if (c === "-" && text[i + 1] === "-") {
      const end = text.indexOf("\n", i);
      const stop = end === -1 ? n : end;
      out += text.slice(i, stop);
      i = stop;
      continue;
    }

    // Block comment: skip to closing */.
    if (c === "/" && text[i + 1] === "*") {
      const end = text.indexOf("*/", i + 2);
      const stop = end === -1 ? n : end + 2;
      out += text.slice(i, stop);
      i = stop;
      continue;
    }

    // Dollar-quoted string ($tag$...$tag$) — includes the empty tag $$...$$.
    const tag = isDollarTagStart(i);
    if (tag) {
      const bodyStart = i + tag.length;
      const end = text.indexOf(tag, bodyStart);
      const stop = end === -1 ? n : end + tag.length;
      out += text.slice(i, stop);
      i = stop;
      continue;
    }

    // Single-quoted literal, honoring '' escapes and backslash escapes
    // (E'...' strings). Double-quoted identifiers use the same loop.
    if (c === "'" || c === '"') {
      const quote = c;
      out += c;
      i++;
      while (i < n) {
        const ch = text[i];
        if (ch === "\\" && quote === "'") {
          out += text.slice(i, i + 2);
          i += 2;
          continue;
        }
        out += ch;
        i++;
        if (ch === quote) {
          if (text[i] === quote) {
            // Escaped quote ('' inside a literal) — keep scanning.
            out += quote;
            i++;
            continue;
          }
          break;
        }
      }
      continue;
    }

    if (c === "?") {
      out += `$${index++}`;
      i++;
      continue;
    }

    out += c;
    i++;
  }
  return out;
}

/** Reads an optional numeric environment variable. */
function optionalEnvNumber(name: string): number | undefined {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return undefined;
  const value = Number(raw);
  return Number.isFinite(value) ? value : undefined;
}

/**
 * Escapes a string literal for safe inclusion in SQL.
 * Local replacement for pg's `escapeLiteral`.
 */
export function escapeLiteral(value: string): string {
  if (value === undefined || value === null) {
    return "NULL";
  }
  let hasBackslash = false;
  let escaped = "'";
  for (const c of value) {
    if (c === "'") {
      escaped += "''";
    } else if (c === "\\") {
      escaped += "\\\\";
      hasBackslash = true;
    } else {
      escaped += c;
    }
  }
  escaped += "'";
  return hasBackslash ? ` E${escaped}` : escaped;
}

const RETRYABLE_SQLSTATES = new Set([
  "40P01" /* deadlock */,
  "40001" /* serialization */,
]);

/**
 * Bun.sql client-level error codes that indicate a transient connection
 * problem (as opposed to a SQL error). Retrying the whole transaction is
 * safe because these failures occur before/without a commit.
 */
const RETRYABLE_CLIENT_CODES = new Set([
  "ERR_POSTGRES_CONNECTION_CLOSED",
  "ERR_POSTGRES_IDLE_TIMEOUT",
  "ERR_POSTGRES_LIFETIME_TIMEOUT",
]);

/** True when the error is a transient failure worth retrying. */
function isRetryable(err: unknown): boolean {
  const code = (err as DatabaseError)?.code;
  return (
    typeof code === "string" &&
    (RETRYABLE_SQLSTATES.has(code) || RETRYABLE_CLIENT_CODES.has(code))
  );
}

/**
 * Handle for queries inside a transaction, bound to the transaction-scoped
 * connection handed out by `sql.begin()`. Lifecycle (BEGIN/COMMIT/ROLLBACK,
 * connection release) is owned by `sql.begin()`; this handle only exposes
 * query execution and savepoint-based nesting.
 */
export class Transaction implements TransactionClient {
  readonly __type = "transaction";
  private savepointCount = 0;

  constructor(
    private readonly client: SQL,
    private readonly owner: PostgresClient,
  ) {}

  async query<T = Record<string, unknown>>(
    sql: string,
    values?: unknown[],
  ): Promise<QueryResult<T>> {
    const result = await this.client.unsafe(
      toPositionalParams(sql),
      bindValues(values),
    );
    return normalizeResult<T>(result);
  }

  async savepoint(): Promise<string> {
    const name = `sp_${++this.savepointCount}`;
    await this.query(`SAVEPOINT ${name}`);
    return name;
  }

  /**
   * Nested transaction — implemented as a savepoint on the same connection.
   */
  async transaction<T>(
    callback: (tx: this) => Promise<T>,
    _maxRetries?: number,
  ): Promise<T> {
    const spName = await this.savepoint();
    try {
      const result = await callback(this);
      await this.releaseSavepoint(spName);
      return result;
    } catch (error) {
      await this.rollbackTo(spName).catch((e) =>
        Logger.error("Rollback to savepoint error", e),
      );
      throw error;
    }
  }

  async releaseSavepoint(name: string) {
    await this.query(`RELEASE SAVEPOINT ${name}`);
  }

  async rollbackTo(name: string) {
    await this.query(`ROLLBACK TO SAVEPOINT ${name}`);
  }

  /** Name of the connected database (delegated to the owning client). */
  get database(): string {
    return this.owner.database;
  }

  /**
   * No-op kept for interface compatibility: transaction-scoped connections
   * are reserved and released by `sql.begin()` itself, so there is nothing
   * to disconnect here.
   */
  async disconnect(): Promise<void> {}

  quote(value: string): string {
    return escapeLiteral(value);
  }

  async ping(): Promise<void> {
    await this.query("SELECT 1");
  }
}

function normalizeResult<T>(result: unknown): QueryResult<T> {
  // Bun.sql returns an array-like of row objects; command results carry a
  // count of affected rows on the array itself.
  const rows = (Array.isArray(result) ? result : []) as T[];
  const rawCount =
    (result as { count?: unknown })?.count ??
    (result as { $count?: unknown })?.$count ??
    rows.length;
  const rowCount = Number(rawCount);
  return { rows, rowCount: Number.isFinite(rowCount) ? rowCount : 0 };
}

/**
 * Serializes a JS array into a Postgres array literal (`{"a","b"}`),
 * matching how node-pg encodes array parameters for `text[]`-style columns.
 * Bun.sql joins arrays with bare commas, which Postgres rejects.
 */
function toPostgresArrayLiteral(value: readonly unknown[]): string {
  const items = value.map((item) => {
    if (item === null || item === undefined) return "NULL";
    const str =
      typeof item === "string"
        ? item
        : typeof item === "object"
          ? JSON.stringify(item)
          : String(item);
    return `"${str.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
  });
  return `{${items.join(",")}}`;
}

/** Prepares bound values for Bun.sql, replicating node-pg serialization. */
function bindValues(values?: unknown[]): unknown[] | undefined {
  return values?.map((value) => {
    // Json attribute values are real jsonb documents: pass them through so
    // the driver serializes them natively (objects AND arrays). Never
    // flatten them into Postgres array literals.
    if (value instanceof JsonParam) return value.value;
    return Array.isArray(value) ? toPostgresArrayLiteral(value) : value;
  });
}

/**
 * Main Database Client — thin wrapper around Bun's native SQL client
 * (`Bun.sql`) providing the legacy query interface used by the adapters.
 */
export class PostgresClient implements QueryClient {
  readonly __type = "postgres";
  private readonly sql: SQL;
  private readonly ownsSql: boolean;
  private readonly databaseName: string;

  constructor(config: SQL | string | Record<string, unknown>) {
    // Hard cap on pooled connections per client (matches Bun's default,
    // overridable via PG_POOL_MAX). Keeps total connections predictable when
    // many clients exist side by side (e.g. test suites).
    const poolMax = Number(process.env["PG_POOL_MAX"] ?? 10);

    // Pool lifecycle tuning, in seconds. Environment variables provide
    // defaults; explicit options in `config` always win.
    const lifecycle: Record<string, number> = {};
    const idleTimeout = optionalEnvNumber("PG_IDLE_TIMEOUT");
    if (idleTimeout !== undefined) lifecycle["idleTimeout"] = idleTimeout;
    const maxLifetime = optionalEnvNumber("PG_MAX_LIFETIME");
    if (maxLifetime !== undefined) lifecycle["maxLifetime"] = maxLifetime;
    const connectionTimeout = optionalEnvNumber("PG_CONNECTION_TIMEOUT");
    if (connectionTimeout !== undefined) {
      lifecycle["connectionTimeout"] = connectionTimeout;
    }

    const onConnect = () => Logger.debug("Postgres connection established");
    // Connection churn (including benign close-time errors) is routine, so
    // everything logs at debug level; real query failures surface through
    // processException with full context.
    const onClose = (err: Error | null) => {
      Logger.debug(
        err
          ? `Postgres connection closed: ${err.message}`
          : "Postgres connection closed",
      );
    };

    if (isSqlClient(config)) {
      this.sql = config;
      this.ownsSql = false;
    } else if (typeof config === "string") {
      this.sql = new SQL({
        url: config,
        max: poolMax,
        ...lifecycle,
        onconnect: onConnect,
        onclose: onClose,
      });
      this.ownsSql = true;
    } else {
      const options = {
        ...lifecycle,
        ...(config as Record<string, unknown>),
      };
      if (options["max"] === undefined) options["max"] = poolMax;
      if (options["onconnect"] === undefined) options["onconnect"] = onConnect;
      if (options["onclose"] === undefined) options["onclose"] = onClose;
      this.sql = new SQL(options as never);
      this.ownsSql = true;
    }
    this.databaseName = extractDatabaseName(this.sql, config);
  }

  async query<T = any>(
    sql: string,
    values?: unknown[],
  ): Promise<QueryResult<T>> {
    const result = await this.sql.unsafe(
      toPositionalParams(sql),
      bindValues(values),
    );
    return normalizeResult<T>(result);
  }

  quote(value: string): string {
    return escapeLiteral(value);
  }

  async ping(): Promise<void> {
    await this.query("SELECT 1");
  }

  /** Name of the connected database (replaces pg Pool options lookup). */
  get database(): string {
    return this.databaseName;
  }

  /**
   * Executes a callback in a safe, isolated transaction on a dedicated
   * pooled connection via Bun's native `sql.begin()`, which owns BEGIN,
   * COMMIT, ROLLBACK and connection release. Retries deadlocks,
   * serialization failures, and transient client-level connection errors.
   */
  async transaction<T>(
    callback: (tx: Transaction) => Promise<T>,
    maxRetries = 3,
  ): Promise<T> {
    let lastError: unknown;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        // Note: the isolation-level string argument is passed through as-is
        // by this Bun build and breaks parsing; omitting it yields
        // PostgreSQL's default READ COMMITTED, which is what we want.
        return await this.sql.begin(async (scoped) => {
          const tx = new Transaction(scoped as unknown as SQL, this);
          return await callback(tx);
        });
      } catch (err: unknown) {
        lastError = err;
        if (isRetryable(err) && attempt < maxRetries) {
          await new Promise((res) =>
            setTimeout(res, Math.pow(2, attempt) * 50),
          );
          continue;
        }
        throw err;
      }
    }
    throw new TransactionException(
      "Transaction failed after max retries.",
      undefined,
      lastError,
    );
  }

  async disconnect(): Promise<void> {
    if (!this.ownsSql) return;
    // Graceful shutdown: wait for in-flight queries to settle before the
    // process tears the pool down. Timeout is in seconds.
    const timeout = optionalEnvNumber("PG_CLOSE_TIMEOUT") ?? 10;
    await this.sql.close({ timeout });
  }
}

function extractDatabaseName(
  sql: SQL,
  config: SQL | string | Record<string, unknown>,
): string {
  if (typeof config === "string") {
    try {
      const url = new URL(config.replace(/^postgres(ql)?:\/\//, "http://"));
      return url.pathname.replace(/^\//, "");
    } catch {
      // fall through
    }
  } else if (config && typeof config === "object") {
    const named = config as { database?: string };
    if (named.database) return named.database;
  }
  // Last resort: ask the server.
  void sql;
  return "";
}
