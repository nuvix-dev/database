import { describe, expect, test } from "bun:test";
import type { SQL } from "bun";
import { PostgresClient } from "@adapters/postgres.js";

describe("PostgresClient ownership", () => {
  test("accepts and does not close a borrowed SQL-compatible client", async () => {
    let closes = 0;
    const sql = {
      unsafe: async () => [{ value: 1 }],
      begin: async () => undefined,
      close: async () => {
        closes += 1;
      },
    } as unknown as SQL;
    const client = new PostgresClient(sql);

    const result = await client.query<{ value: number }>("SELECT 1 AS value");
    await client.disconnect();

    expect(result.rows).toEqual([{ value: 1 }]);
    expect(closes).toBe(0);
  });
});
