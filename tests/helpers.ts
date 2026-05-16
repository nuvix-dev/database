import { SQL } from "bun";
import { Adapter } from "@adapters/adapter.js";
import { Database } from "@core/database.js";
import { Authorization } from "@utils/authorization.js";
import { Cache as NuvixCache, Redis } from "@nuvix/cache";

export function createTestAdapter(
  meta?: Partial<{
    database: string;
    schema: string;
    sharedTables: boolean;
    tenantId: number;
    tenantPerDocument: boolean;
    namespace: string;
  }>,
): Adapter {
  const connectionString =
    process.env["PG_URL"] ||
    "postgres://user:password@localhost:5432/postgres";
  const sql = new SQL(connectionString);
  const adapter = new Adapter(sql);
  adapter.setMeta({
    schema: meta?.schema || "public",
    sharedTables: meta?.sharedTables ?? false,
    tenantId: meta?.tenantId ?? 1,
    tenantPerDocument: meta?.tenantPerDocument ?? false,
    namespace: meta?.namespace || "tests",
  });
  return adapter;
}

export function createTestDb(
  meta?: Partial<{
    sharedTables: boolean;
    tenantId: number;
    tenantPerDocument: boolean;
    namespace: string;
  }>,
): Database {
  const adapter = createTestAdapter(meta);
  const cache = new NuvixCache(new Redis({}));
  Authorization.setDefaultStatus(false); // disable auth by default in tests
  return new Database(adapter, cache);
}
