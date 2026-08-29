import { afterEach, describe, expect, test } from "bun:test";
import type { QueryClient } from "@adapters/interface.js";
import type { Database } from "@core/database.js";
import { Doc } from "@core/doc.js";
import { AttributeEnum, EventsEnum } from "@core/enums.js";
import type { Session } from "@core/session.js";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";
import { createSQLiteTestDb } from "./sqlite-helpers.js";

const databases: Database[] = [];

afterEach(async () => {
  await Promise.all(
    databases.splice(0).map((database) =>
      database.getAdapter().$client.disconnect(),
    ),
  );
});

function sessionDatabase(session: Session): Database {
  return (session as unknown as { database: Database }).database;
}

async function setup(): Promise<Database> {
  const database = createSQLiteTestDb({ namespace: "transactions" });
  databases.push(database);
  await database.create("main");
  await database.createCollection({
    id: "records",
    attributes: [
      new Doc({
        $id: "name",
        key: "name",
        type: AttributeEnum.String,
        size: 128,
        required: true,
      }),
      new Doc({
        $id: "score",
        key: "score",
        type: AttributeEnum.Integer,
        required: true,
      }),
    ],
    permissions: [
      Permission.create(Role.user("alice")),
      Permission.read(Role.user("alice")),
      Permission.update(Role.user("alice")),
    ],
    documentSecurity: false,
  });
  return database;
}

describe("SQLite session transactions", () => {
  test("commits every operation in a successful multi-operation transaction", async () => {
    // Arrange
    const database = await setup();
    const session = database.system();

    // Act
    const result = await session.withTransaction(async (transaction) => {
      await transaction.createDocument(
        "records",
        new Doc({ $id: "commit-one", name: "Commit one", score: 1 }),
      );
      await transaction.createDocument(
        "records",
        new Doc({ $id: "commit-two", name: "Commit two", score: 2 }),
      );
      return transaction.sum("records", "score");
    });

    // Assert
    expect(result).toBe(3);
    expect(
      (await session.find("records"))
        .map((document) => document.getId())
        .sort(),
    ).toEqual(["commit-one", "commit-two"]);
  });

  test("rolls back every operation when a multi-operation transaction fails", async () => {
    // Arrange
    const database = await setup();
    const session = database.system();

    // Act
    const transaction = session.withTransaction(async (scope) => {
      await scope.createDocument(
        "records",
        new Doc({ $id: "rollback-one", name: "Rollback one", score: 1 }),
      );
      await scope.createDocument(
        "records",
        new Doc({ $id: "rollback-two", name: "Rollback two", score: 2 }),
      );
      throw new Error("abort all writes");
    });

    // Assert
    await expect(transaction).rejects.toThrow("abort all writes");
    expect(await session.count("records")).toBe(0);
  });

  test("uses nested savepoints so caught failures preserve successful outer work", async () => {
    // Arrange
    const database = await setup();
    const session = database.system();

    // Act
    await session.withTransaction(async (outer) => {
      const outerDatabase = sessionDatabase(outer);
      await outer.createDocument(
        "records",
        new Doc({ $id: "outer-before", name: "Outer before", score: 1 }),
      );
      await outer.withTransaction(async (nested) => {
        expect(sessionDatabase(nested)).toBe(outerDatabase);
        await nested.createDocument(
          "records",
          new Doc({ $id: "nested-commit", name: "Nested commit", score: 2 }),
        );
      });
      await expect(
        outer.withTransaction(async (nested) => {
          expect(sessionDatabase(nested)).toBe(outerDatabase);
          await nested.createDocument(
            "records",
            new Doc({
              $id: "nested-rollback",
              name: "Nested rollback",
              score: 3,
            }),
          );
          throw new Error("rollback nested savepoint");
        }),
      ).rejects.toThrow("rollback nested savepoint");
      await outer.createDocument(
        "records",
        new Doc({ $id: "outer-after", name: "Outer after", score: 4 }),
      );
    });

    // Assert
    expect(
      (await session.find("records"))
        .map((document) => document.getId())
        .sort(),
    ).toEqual(["nested-commit", "outer-after", "outer-before"]);
  });

  test("preserves session state while isolating transaction-local mutations", async () => {
    // Arrange
    const database = createSQLiteTestDb({ namespace: "scope-parent" });
    databases.push(database);
    const encodedBy: Database[] = [];
    const parentEvents: string[] = [];
    const transactionEvents: string[] = [];
    const parentTransformations: string[] = [];
    const transactionTransformations: string[] = [];
    database.addFilter("transaction-filter", {
      encode(value, _document, scope) {
        encodedBy.push(scope);
        return `encoded:${String(value)}`;
      },
      decode(value) {
        return String(value).replace(/^encoded:/, "");
      },
    });
    database.setMeta({ metadata: { request: "parent" } });
    database.on(EventsEnum.DocumentCreate, "parent-listener", (document) => {
      parentEvents.push(document.getId());
    });
    database.before(EventsEnum.DocumentCreate, "parent-transform", (sql) => {
      parentTransformations.push(sql);
      return sql;
    });
    await database.create("main");
    await database.createCollection({
      id: "filtered",
      attributes: [
        new Doc({
          $id: "value",
          key: "value",
          type: AttributeEnum.String,
          size: 128,
          required: true,
          filters: ["transaction-filter"],
        }),
      ],
      permissions: [
        Permission.create(Role.user("alice")),
        Permission.read(Role.user("alice")),
      ],
      documentSecurity: false,
    });
    parentEvents.length = 0;
    parentTransformations.length = 0;
    const session = database.for("user:alice");

    // Act
    await session.withTransaction(async (transaction) => {
      const transactionDatabase = sessionDatabase(transaction);
      expect(transaction.ctx).toBe(session.ctx);
      expect(transactionDatabase).not.toBe(database);
      expect(transactionDatabase.metadata).toEqual({ request: "parent" });
      expect(transactionDatabase.getFilters()["transaction-filter"]).toBe(
        database.getFilters()["transaction-filter"],
      );

      transactionDatabase.on(
        EventsEnum.DocumentCreate,
        "transaction-listener",
        (document) => transactionEvents.push(document.getId()),
      );
      transactionDatabase.before(
        EventsEnum.DocumentCreate,
        "transaction-transform",
        (sql) => {
          transactionTransformations.push(sql);
          return sql;
        },
      );
      transactionDatabase.addFilter("transaction-only-filter", {
        encode: (value) => value,
        decode: (value) => value,
      });

      const created = await transaction.createDocument(
        "filtered",
        new Doc({ $id: "inside", value: "visible" }),
      );
      expect(created.get("value")).toBe("visible");
      expect(encodedBy.at(-1)).toBe(transactionDatabase);

      transactionDatabase.setMeta({
        metadata: { request: "transaction" },
      });
      expect(transactionDatabase.metadata).toEqual({
        request: "transaction",
      });
    });
    await session.createDocument(
      "filtered",
      new Doc({ $id: "outside", value: "parent" }),
    );
    const unauthorized = database
      .for("user:bob")
      .withTransaction((transaction) =>
        transaction.createDocument(
          "filtered",
          new Doc({ $id: "denied", value: "denied" }),
        ),
      );

    // Assert
    await expect(unauthorized).rejects.toThrow();
    expect(parentEvents).toEqual(["inside", "outside"]);
    expect(transactionEvents).toEqual(["inside"]);
    expect(parentTransformations).toHaveLength(2);
    expect(transactionTransformations).toHaveLength(1);
    expect(database.metadata).toEqual({ request: "parent" });
    expect(database.getFilters()["transaction-only-filter"]).toBeUndefined();
    expect(await session.count("filtered")).toBe(2);
  });

  test("routes callback operations through one active transaction handle and retires it", async () => {
    // Arrange
    const database = await setup();
    const session = database.system();
    const parentAdapter = database.getAdapter();
    let transactionClient: QueryClient | undefined;
    const statements: string[] = [];

    // Act
    await session.withTransaction(async (transaction) => {
      const transactionDatabase = sessionDatabase(transaction);
      const adapter = transactionDatabase.getAdapter();
      transactionClient = adapter.$client;
      const client = transactionClient as QueryClient;
      const originalQuery = client.query.bind(client);
      client.query = async (sql, values) => {
        statements.push(sql);
        return originalQuery(sql, values);
      };

      expect(transactionDatabase).not.toBe(database);
      expect(adapter).not.toBe(parentAdapter);
      expect(client.__type).toBe("transaction");

      const beforeCreate = statements.length;
      await transaction.createDocument(
        "records",
        new Doc({ $id: "handled", name: "Handled", score: 5 }),
      );
      expect(statements.length).toBeGreaterThan(beforeCreate);

      const beforeRead = statements.length;
      expect(await transaction.count("records")).toBe(1);
      expect(statements.length).toBeGreaterThan(beforeRead);

      await transaction.withTransaction(async (nested) => {
        expect(sessionDatabase(nested).getAdapter()).toBe(adapter);
        await nested.find("records");
      });
    });

    // Assert
    expect(statements.some((sql) => sql.startsWith("SAVEPOINT"))).toBe(true);
    expect(transactionClient).toBeDefined();
    await expect(transactionClient!.query("SELECT 1")).rejects.toThrow(
      "SQLite transaction is no longer active",
    );
    await expect(parentAdapter.$client.ping()).resolves.toBeUndefined();
  });
});
