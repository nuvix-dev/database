import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "bun:sqlite";
import { describe, expect, test } from "bun:test";
import {
  escapeSQLiteLiteral,
  SQLiteClient,
} from "@adapters/sqlite.js";
import { DuplicateException } from "@errors/index.js";

describe("SQLiteClient", () => {
  test("accepts :memory: and reports its database identity", async () => {
    const client = new SQLiteClient(":memory:");

    try {
      expect(client.database).toBe(":memory:");
      await expect(client.ping()).resolves.toBeUndefined();
    } finally {
      await client.disconnect();
    }
  });

  test("accepts a filesystem path and persists committed data", async () => {
    const directory = await mkdtemp(join(tmpdir(), "nuvix-sqlite-"));
    const path = join(directory, "client.sqlite");
    const client = new SQLiteClient(path);

    try {
      expect(client.database).toBe(path);
      await client.query("CREATE TABLE records (value TEXT NOT NULL)");
      await client.query("INSERT INTO records (value) VALUES (?)", ["saved"]);
      await client.disconnect();

      const reopened = new Database(path);
      try {
        expect(
          reopened.query<{ value: string }, []>("SELECT value FROM records").get(),
        ).toEqual({ value: "saved" });
      } finally {
        reopened.close(true);
      }
    } finally {
      await client.disconnect();
      await rm(directory, { recursive: true, force: true });
    }
  });

  test("does not close a borrowed Database handle", async () => {
    const database = new Database(":memory:");
    const client = new SQLiteClient(database);

    try {
      await client.query("CREATE TABLE borrowed (id INTEGER)");
      await client.disconnect();

      expect(
        database.query<{ value: number }, []>("SELECT 1 AS value").get(),
      ).toEqual({ value: 1 });
    } finally {
      database.close(true);
    }
  });

  test("normalizes SELECT and INSERT RETURNING rows", async () => {
    const client = new SQLiteClient(":memory:");

    try {
      await client.query("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)");
      const inserted = await client.query<{ id: number; name: string }>(
        "INSERT INTO items (name) VALUES (?) RETURNING id, name",
        ["bound"],
      );
      const selected = await client.query<{ id: number; name: string }>(
        "SELECT id, name FROM items WHERE name = ?",
        ["bound"],
      );

      expect(inserted).toEqual({
        rows: [{ id: 1, name: "bound" }],
        rowCount: 1,
      });
      expect(selected).toEqual(inserted);
    } finally {
      await client.disconnect();
    }
  });

  test("normalizes UPDATE and DELETE affected-row counts", async () => {
    const client = new SQLiteClient(":memory:");

    try {
      await client.query("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)");
      await client.query("INSERT INTO items (name) VALUES (?), (?)", ["a", "b"]);

      const updated = await client.query("UPDATE items SET name = ?", ["updated"]);
      const deleted = await client.query("DELETE FROM items WHERE id = ?", [1]);

      expect(updated).toEqual({ rows: [], rowCount: 2 });
      expect(deleted).toEqual({ rows: [], rowCount: 1 });
    } finally {
      await client.disconnect();
    }
  });

  test("maps bun:sqlite query failures through the shared exceptions", async () => {
    const client = new SQLiteClient(":memory:");

    try {
      await client.query("CREATE TABLE users (email TEXT UNIQUE)");
      await client.query("INSERT INTO users (email) VALUES (?)", ["a@example.com"]);

      await expect(
        client.query("INSERT INTO users (email) VALUES (?)", ["a@example.com"]),
      ).rejects.toBeInstanceOf(DuplicateException);
    } finally {
      await client.disconnect();
    }
  });

  test("commits an outer async transaction", async () => {
    const client = new SQLiteClient(":memory:");

    try {
      await client.query("CREATE TABLE events (id INTEGER)");
      await client.transaction(async (transaction) => {
        await Promise.resolve();
        await transaction.query("INSERT INTO events VALUES (?)", [1]);
      });

      const result = await client.query<{ count: number }>(
        "SELECT count(*) AS count FROM events",
      );
      expect(result.rows[0]?.count).toBe(1);
    } finally {
      await client.disconnect();
    }
  });

  test("rolls back an outer transaction when its callback fails", async () => {
    const client = new SQLiteClient(":memory:");

    try {
      await client.query("CREATE TABLE events (id INTEGER)");
      await expect(
        client.transaction(async (transaction) => {
          await transaction.query("INSERT INTO events VALUES (?)", [1]);
          await Promise.resolve();
          throw new Error("outer failure");
        }),
      ).rejects.toThrow("outer failure");

      const result = await client.query<{ count: number }>(
        "SELECT count(*) AS count FROM events",
      );
      expect(result.rows[0]?.count).toBe(0);
    } finally {
      await client.disconnect();
    }
  });

  test("uses unique savepoints and rolls back only failed nested work", async () => {
    const client = new SQLiteClient(":memory:");

    try {
      await client.query("CREATE TABLE events (id INTEGER)");
      await client.transaction(async (outer) => {
        await outer.query("INSERT INTO events VALUES (?)", [1]);
        const first = await outer.transaction(async (nested) => {
          await nested.query("INSERT INTO events VALUES (?)", [2]);
          return "committed";
        });
        expect(first).toBe("committed");

        await expect(
          outer.transaction(async (nested) => {
            await nested.query("INSERT INTO events VALUES (?)", [3]);
            throw new Error("nested failure");
          }),
        ).rejects.toThrow("nested failure");
      });

      const result = await client.query<{ id: number }>(
        "SELECT id FROM events ORDER BY id",
      );
      expect(result.rows.map(({ id }) => id)).toEqual([1, 2]);
    } finally {
      await client.disconnect();
    }
  });

  test("quotes SQLite string literals safely", async () => {
    const client = new SQLiteClient(":memory:");

    try {
      const quoted = client.quote("it's \\ safe");
      expect(quoted).toBe("'it''s \\ safe'");
      expect(escapeSQLiteLiteral("a'b")).toBe("'a''b'");

      const result = await client.query<{ value: string }>(
        `SELECT ${quoted} AS value`,
      );
      expect(result.rows[0]?.value).toBe("it's \\ safe");
    } finally {
      await client.disconnect();
    }
  });

  test("disconnects owned handles idempotently", async () => {
    const client = new SQLiteClient(":memory:");

    await client.disconnect();
    await expect(client.disconnect()).resolves.toBeUndefined();
    await expect(client.ping()).rejects.toThrow("SQLite client is disconnected");
  });
});
