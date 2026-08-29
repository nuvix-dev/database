import { Cache, Memory } from "@nuvix/cache";
import { SQLiteAdapter } from "@adapters/sqlite-adapter.js";
import { Database } from "@core/database.js";

type SQLiteTestMeta = Partial<{
  schema: string;
  sharedTables: boolean;
  tenantId: number;
  tenantPerDocument: boolean;
  namespace: string;
}>;

export function createSQLiteTestAdapter(
  meta: SQLiteTestMeta = {},
): SQLiteAdapter {
  const adapter = new SQLiteAdapter(":memory:");
  adapter.setMeta({
    schema: meta.schema ?? "main",
    sharedTables: meta.sharedTables ?? false,
    tenantId: meta.tenantId ?? 1,
    tenantPerDocument: meta.tenantPerDocument ?? false,
    namespace: meta.namespace ?? "tests",
  });
  return adapter;
}

export function createSQLiteTestDb(meta: SQLiteTestMeta = {}): Database {
  return new Database(
    createSQLiteTestAdapter(meta),
    new Cache(new Memory()),
  );
}
