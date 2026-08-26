import { afterAll } from "bun:test";
import { Adapter } from "@adapters/adapter.js";
import { Database } from "@core/database.js";
import { Cache as NuvixCache, Redis } from "@nuvix/cache";

const PG_URL =
  process.env["PG_URL"] || "postgres://user:password@localhost:5432/postgres";

// Tracks adapters created in this test file so pools get closed instead of
// accumulating. Many suites call createTestDb() inside beforeEach(), spawning
// a fresh pool per test; without eviction, hundreds of Postgres connections
// pile up until the server refuses more ("sorry, too many clients already").
// We keep only the most recent few alive — older ones belong to tests that
// have already finished, so closing them is safe. Suites that create a single
// shared Database never hit the cap.
const MAX_OPEN_ADAPTERS = 4;
const openAdapters: Adapter[] = [];

function evictStaleAdapters(): void {
  while (openAdapters.length >= MAX_OPEN_ADAPTERS) {
    const oldest = openAdapters.shift();
    if (!oldest) break;
    void oldest.$client.disconnect().catch(() => {
      // pool may already be closed — nothing to do
    });
  }
}

try {
  afterAll(async () => {
    for (const adapter of openAdapters.splice(0)) {
      try {
        await adapter.$client.disconnect();
      } catch {
        // pool may already be closed — nothing to do
      }
    }
  });
} catch {
  // Not running under `bun test` (e.g. ad-hoc script importing helpers).
}

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
  evictStaleAdapters();
  const adapter = new Adapter(PG_URL);
  adapter.setMeta({
    schema: meta?.schema || "public",
    sharedTables: meta?.sharedTables ?? false,
    tenantId: meta?.tenantId ?? 1,
    tenantPerDocument: meta?.tenantPerDocument ?? false,
    namespace: meta?.namespace || "tests",
  });
  openAdapters.push(adapter);
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
  return new Database(adapter, cache);
}
