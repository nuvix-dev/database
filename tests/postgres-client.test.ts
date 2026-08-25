import { describe, test, expect, afterAll } from "bun:test";
import { toPositionalParams, PostgresClient } from "@adapters/postgres.js";
import { createTestAdapter } from "./helpers.js";

describe("toPositionalParams", () => {
  test("rewrites plain placeholders in order", () => {
    expect(toPositionalParams("SELECT * FROM t WHERE a = ? AND b = ?")).toBe(
      "SELECT * FROM t WHERE a = $1 AND b = $2",
    );
  });

  test("leaves queries without placeholders untouched", () => {
    const sql = "SELECT 1";
    expect(toPositionalParams(sql)).toBe(sql);
  });

  test("ignores ? inside single-quoted literals", () => {
    expect(toPositionalParams("SELECT 'a?b' AS v FROM t WHERE x = ?")).toBe(
      "SELECT 'a?b' AS v FROM t WHERE x = $1",
    );
  });

  test("handles escaped quotes followed by ?", () => {
    expect(
      toPositionalParams("SELECT 'it''s a ? test' WHERE x = ? AND y = ?"),
    ).toBe("SELECT 'it''s a ? test' WHERE x = $1 AND y = $2");
  });

  test("handles backslash escapes in E-strings", () => {
    expect(toPositionalParams("SELECT E'a\\?b' WHERE x = ?")).toBe(
      "SELECT E'a\\?b' WHERE x = $1",
    );
  });

  test("ignores ? inside double-quoted identifiers", () => {
    expect(toPositionalParams('SELECT "col?" FROM t WHERE x = ?')).toBe(
      'SELECT "col?" FROM t WHERE x = $1',
    );
  });

  test("ignores ? inside line comments", () => {
    expect(toPositionalParams("-- what? \nSELECT 1 WHERE x = ?")).toBe(
      "-- what? \nSELECT 1 WHERE x = $1",
    );
  });

  test("ignores ? inside block comments", () => {
    expect(toPositionalParams("/* is this? */ SELECT 1 WHERE x = ?")).toBe(
      "/* is this? */ SELECT 1 WHERE x = $1",
    );
  });

  test("ignores ? inside dollar-quoted bodies", () => {
    expect(
      toPositionalParams("SELECT $$literal ? here$$ AS v WHERE x = ?"),
    ).toBe("SELECT $$literal ? here$$ AS v WHERE x = $1");
  });

  test("ignores ? inside tagged dollar-quoted bodies", () => {
    expect(
      toPositionalParams("SELECT $fn$ body with ? marks $fn$ AS v WHERE x = ?"),
    ).toBe("SELECT $fn$ body with ? marks $fn$ AS v WHERE x = $1");
  });
});

describe("PostgresClient integration", () => {
  const client = new PostgresClient(
    process.env["PG_URL"] ||
      "postgres://postgres:postgres@localhost:5432/nuvix_test",
  );

  afterAll(async () => {
    await client.disconnect();
  });

  test("binds parameters while preserving ? inside literals", async () => {
    const { rows } = await client.query(
      "SELECT 'a?b' AS literal, ?::text AS bound",
      ["hello"],
    );
    expect(rows[0]["literal"]).toBe("a?b");
    expect(rows[0]["bound"]).toBe("hello");
  });

  test("commits transactions via sql.begin", async () => {
    await client.query("DROP TABLE IF EXISTS __tx_smoke");
    await client.query("CREATE TABLE __tx_smoke (id int)");

    await client.transaction(async (tx) => {
      await tx.query("INSERT INTO __tx_smoke VALUES (?)", [1]);
    });

    const { rows } = await client.query(
      "SELECT count(*)::int AS n FROM __tx_smoke",
    );
    expect(rows[0]["n"]).toBe(1);
  });

  test("rolls back transactions on failure via sql.begin", async () => {
    await client.query("DROP TABLE IF EXISTS __tx_smoke_rb");
    await client.query("CREATE TABLE __tx_smoke_rb (id int)");

    await expect(
      client.transaction(async (tx) => {
        await tx.query("INSERT INTO __tx_smoke_rb VALUES (?)", [1]);
        throw new Error("boom");
      }),
    ).rejects.toThrow("boom");

    const { rows } = await client.query(
      "SELECT count(*)::int AS n FROM __tx_smoke_rb",
    );
    expect(rows[0]["n"]).toBe(0);
  });

  test("supports nested transactions via savepoints", async () => {
    await client.query("DROP TABLE IF EXISTS __tx_nested");
    await client.query("CREATE TABLE __tx_nested (id int)");

    // Inner failure rolls back only the savepoint; outer insert survives.
    await client.transaction(async (outer) => {
      await outer.query("INSERT INTO __tx_nested VALUES (?)", [1]);
      await expect(
        outer.transaction(async (inner) => {
          await inner.query("INSERT INTO __tx_nested VALUES (?)", [2]);
          throw new Error("inner boom");
        }),
      ).rejects.toThrow("inner boom");
    });

    const { rows } = await client.query(
      "SELECT id FROM __tx_nested ORDER BY id",
    );
    expect(rows.map((r) => r["id"])).toEqual([1]);
  });
});

describe("adapter transaction facade", () => {
  const adapter = createTestAdapter();

  afterAll(async () => {
    await adapter.$client.disconnect();
  });

  test("runs queries inside adapter.transaction via sql.begin", async () => {
    const result = await adapter.transaction(async (txAdapter) => {
      const { rows } = await txAdapter.$client.query("SELECT 1 AS one");
      return rows[0]["one"];
    });
    expect(result).toBe(1);
  });

  test("propagates and rolls back when the callback throws", async () => {
    await expect(
      adapter.transaction(async () => {
        throw new Error("facade boom");
      }),
    ).rejects.toThrow("facade boom");
  });
});
