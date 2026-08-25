/**
 * Benchmark: local @nuvix/db (source) vs published npm @nuvix/db@0.2.5.
 *
 * Standalone sub-project — deps live in benchmarks/package.json.
 * Run:   cd benchmarks && bun install && bun run bench
 * Needs: PostgreSQL reachable via PG_URL (default: local docker container).
 *
 * Each library gets its own namespace + collection so runs never interfere.
 * The JSON-path scenario doubles as a correctness check: npm 0.2.5 suffers
 * the jsonb double-encoding bug fixed in this repo, so it should report
 * 0 matching rows where the local build finds them.
 */
import { performance } from "node:perf_hooks";

import * as Local from "../src/index.ts";
import * as Npm from "@nuvix/db-npm";
import { Cache as NuvixCache, Memory } from "@nuvix/cache";
import pg from "pg";

const PG_URL =
  process.env["PG_URL"] ??
  "postgres://postgres:postgres@localhost:5432/nuvix_test";

const INSERTS = 300;
const FINDS = 200;
const GETS = 300;
const UPDATES = 200;
const WARMUP = 20;

type Lib = typeof Local;

interface BenchResult {
  scenario: string;
  ops: number;
  ms: number;
  opsPerSec: number;
}

function makeDb(lib: Lib, namespace: string, usePgPool = false) {
  // Local lib: Bun SQL connection string (object-config form currently
  // crashes the local Adapter — `config instanceof SQL` throws on plain
  // objects under Bun). npm 0.2.5: wraps a node-pg Pool instead.
  const adapter = usePgPool
    ? new lib.Adapter(new pg.Pool({ connectionString: PG_URL }))
    : new lib.Adapter(PG_URL);
  adapter.setMeta({
    schema: "public",
    sharedTables: false,
    tenantId: 1,
    tenantPerDocument: false,
    namespace,
  });
  const db = new lib.Database(adapter, new NuvixCache(new Memory()));
  lib.Authorization.setDefaultStatus(false);
  return { adapter, db };
}

function attributes(lib: Lib) {
  const { Doc } = lib;
  // Public API exports the enum as AttributeType; fall back for older builds.
  const T =
    (lib as unknown as Record<string, never>)["AttributeType"] ??
    (lib as unknown as Record<string, never>)["AttributeEnum"];
  return [
    new Doc({
      $id: "name",
      key: "name",
      type: T.String,
      size: 100,
      required: true,
    }),
    new Doc({ $id: "age", key: "age", type: T.Integer, size: 4 }),
    new Doc({
      $id: "tags",
      key: "tags",
      type: T.String,
      size: 50,
      array: true,
    }),
    new Doc({ $id: "meta", key: "meta", type: T.Json }),
  ];
}

async function timeScenario(
  label: string,
  ops: number,
  fn: (i: number) => Promise<unknown>,
): Promise<BenchResult> {
  for (let i = 0; i < Math.min(WARMUP, ops); i++) await fn(i);
  const start = performance.now();
  for (let i = 0; i < ops; i++) await fn(i);
  const ms = performance.now() - start;
  return { scenario: label, ops, ms, opsPerSec: ops / (ms / 1000) };
}

async function benchLib(
  name: string,
  lib: Lib,
  namespace: string,
  usePgPool = false,
): Promise<{ results: BenchResult[]; jsonPathRows: number }> {
  const { adapter, db } = makeDb(lib, namespace, usePgPool);
  const collectionId = `${namespace}_bench`;
  const results: BenchResult[] = [];
  let jsonPathRows = -1;

  // A failing scenario (e.g. npm 0.2.5 rejecting JSON-path queries in its
  // validator) must not abort the whole comparison — record and continue.
  const push = async (
    label: string,
    ops: number,
    fn: (i: number) => Promise<unknown>,
  ) => {
    try {
      results.push(await timeScenario(label, ops, fn));
    } catch (e) {
      results.push({
        scenario: `${label} [FAILED]`,
        ops: 0,
        ms: 0,
        opsPerSec: 0,
      });
      console.log(
        `  ! ${name}: "${label}" failed: ${(e as Error).message.split("\n")[0].slice(0, 120)}`,
      );
    }
  };

  try {
    await db.create();
    await db.createCollection({
      id: collectionId,
      attributes: attributes(lib),
      permissions: [lib.Permission.create(lib.Role.any())],
    });

    let seq = 0;
    await push("insertDocument", INSERTS, async () => {
      seq++;
      await db.createDocument(
        collectionId,
        new lib.Doc({
          name: `user-${seq}`,
          age: 20 + (seq % 50),
          tags: ["tag" + (seq % 5)],
          meta:
            seq % 2 === 0
              ? { details: { hobbies: ["traveling", "gaming"] } }
              : { details: { hobbies: ["reading"] } },
        }),
      );
    });

    await push("find (equal age=25)", FINDS, () =>
      db.find(collectionId, [lib.Query.equal("age", [25])]),
    );

    await push("find (json path contains)", FINDS, () =>
      db.find(collectionId, [
        lib.Query.contains("meta->details->>hobbies", ["traveling"]),
      ]),
    );

    try {
      jsonPathRows = (
        await db.find(collectionId, [
          lib.Query.contains("meta->details->>hobbies", ["traveling"]),
        ])
      ).length;
    } catch {
      jsonPathRows = -1; // query rejected/errored
    }

    const ids: string[] = [];
    const sample = await db.find(collectionId, [lib.Query.limit(GETS)]);
    for (const doc of sample) ids.push(doc.getId());

    let gi = 0;
    await push("getDocument", GETS, () =>
      db.getDocument(collectionId, ids[gi++ % ids.length]),
    );

    let ui = 0;
    await push("updateDocument", UPDATES, () => {
      ui++;
      return db.updateDocument(
        collectionId,
        ids[ui % ids.length],
        new lib.Doc({ age: 40 + (ui % 10) }),
      );
    });
  } finally {
    try {
      await db.deleteCollection(collectionId);
    } catch {
      /* best effort */
    }
    try {
      await adapter.$client?.disconnect?.();
    } catch {
      /* npm 0.2.5 disconnect() can throw on its own internals */
    }
  }

  return { results, jsonPathRows };
}

function printTable(label: string, results: BenchResult[]) {
  console.log(`\n${label}`);
  console.log(
    "  scenario".padEnd(30),
    "ops".padStart(6),
    "total ms".padStart(10),
    "ops/sec".padStart(10),
  );
  for (const r of results) {
    console.log(
      "  " + r.scenario.padEnd(29),
      String(r.ops).padStart(6),
      r.ms.toFixed(1).padStart(10),
      r.opsPerSec.toFixed(0).padStart(10),
    );
  }
}

async function main() {
  console.log(`@nuvix/db benchmark — local source vs npm@0.2.5`);
  console.log(`target: ${PG_URL}\n`);

  const local = await benchLib(
    "local (source)",
    Local as unknown as Lib,
    `bl_${Date.now()}`,
  );
  const npm = await benchLib(
    "npm 0.2.5",
    Npm as unknown as Lib,
    `bn_${Date.now()}`,
    true, // npm 0.2.5 Adapter wraps a node-pg Pool
  );

  printTable("local (source, v0.3.0 + fixes)", local.results);
  printTable("npm @nuvix/db@0.2.5", npm.results);

  console.log(
    "\nJSON-path query correctness (meta->details->>hobbies contains 'traveling'):",
  );
  const fmt = (v: number) => (v < 0 ? "n/a (query rejected)" : `${v} rows`);
  console.log(`  local : ${fmt(local.jsonPathRows)}`);
  console.log(`  npm   : ${fmt(npm.jsonPathRows)}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
