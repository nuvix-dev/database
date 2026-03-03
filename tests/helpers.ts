import { Adapter } from "@adapters/adapter.js";
import { Database } from "@core/database.js";
import { Authorization } from "@utils/authorization.js";
import { Cache as NuvixCache, Redis } from "@nuvix/cache";
import { Pool } from "pg";

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
  const config = new Pool({
    connectionString:
      process.env["PG_URL"] ||
      "postgres://user:password@localhost:5432/postgres",
  });
  const adapter = new Adapter(config);
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
