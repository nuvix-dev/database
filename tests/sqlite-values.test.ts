import { describe, expect, test } from "bun:test";
import { bindSQLiteValue } from "@adapters/sqlite-values.js";
import { JsonParam } from "@adapters/types.js";
import { Doc } from "@core/doc.js";

describe("SQLite value binding", () => {
  test("serializes Doc instances nested in JSON arrays and objects", () => {
    const value = new JsonParam({
      attributes: [
        new Doc({
          $id: "name",
          options: { nested: new Doc({ enabled: true }) },
        }),
      ],
    });

    expect(JSON.parse(bindSQLiteValue(value) as string)).toEqual({
      attributes: [
        {
          $id: "name",
          options: { nested: { enabled: true } },
        },
      ],
    });
  });

  test("continues to reject unrelated class instances", () => {
    class Unsupported {}

    expect(() => bindSQLiteValue(new JsonParam(new Unsupported()))).toThrow(
      "unsupported value type Unsupported",
    );
  });

  test("continues to reject circular values containing Docs", () => {
    const value: { child?: Doc<any> } = {};
    value.child = new Doc<any>({ parent: value });

    expect(() => bindSQLiteValue(new JsonParam(value))).toThrow(
      "circular reference",
    );
  });
});
