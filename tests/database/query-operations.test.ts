import {
  describe,
  test,
  expect,
  beforeEach,
  beforeAll,
  afterAll,
} from "bun:test";
import { Database } from "@core/database.js";
import { createTestDb } from "../helpers.js";
import { Doc } from "@core/doc.js";
import { Query } from "@core/query.js";
import { AttributeEnum, IndexEnum } from "@core/enums.js";
import { Attribute, Index } from "@validators/schema.js";

describe("Query Operations", () => {
  let db: Database;
  let system: ReturnType<Database["system"]>;
  let testCollectionId: string;
  let testDocuments: Doc<any>[];

  const schema = new Date().getTime().toString();

  beforeAll(async () => {
    db = createTestDb({ namespace: `coll_op_${schema}` });
    db.setMeta({ schema });
    await db.create();
    system = db.system();
  });

  beforeEach(async () => {
    testCollectionId = `query_test_${Date.now()}`;

    // Create test collection with various attribute types
    await db.createCollection({
      id: testCollectionId,
      attributes: [
        new Doc<Attribute>({
          $id: "name",
          key: "name",
          type: AttributeEnum.String,
          size: 255,
          required: true,
        }),
        new Doc<Attribute>({
          $id: "age",
          key: "age",
          type: AttributeEnum.Integer,
          required: false,
          default: 0,
        }),
        new Doc<Attribute>({
          $id: "email",
          key: "email",
          type: AttributeEnum.String,
          size: 255,
          required: false,
        }),
        new Doc<Attribute>({
          $id: "active",
          key: "active",
          type: AttributeEnum.Boolean,
          required: false,
          default: true,
        }),
        new Doc<Attribute>({
          $id: "score",
          key: "score",
          type: AttributeEnum.Float,
          required: false,
        }),
        new Doc<Attribute>({
          $id: "department",
          key: "department",
          type: AttributeEnum.String,
          size: 100,
          required: false,
        }),
        new Doc<Attribute>({
          $id: "tags",
          key: "tags",
          type: AttributeEnum.String,
          size: 100,
          array: true,
          required: false,
        }),
        new Doc<Attribute>({
          $id: "meta",
          key: "meta",
          type: AttributeEnum.Json,
          required: false,
        }),
      ],
      indexes: [
        new Doc<Index>({
          $id: "_name",
          type: IndexEnum.FullText,
          attributes: ["name"],
        }),
      ],
    });

    // Create test documents with varied data
    const documentsData = [
      {
        name: "Alice Johnson",
        age: 25,
        email: "alice@example.com",
        active: true,
        score: 85.5,
        department: "Engineering",
        tags: ["javascript", "react"],
        meta: {
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
          notes: "Top performer",
          details: {
            hobbies: ["coding", "reading"],
            location: "NYC",
          },
        },
      },
      {
        name: "Bob Smith",
        age: 30,
        email: "bob@example.com",
        active: true,
        score: 92.0,
        department: "Engineering",
        tags: ["python", "django"],
        meta: {
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
          notes: "Experienced developer",
          details: {
            hobbies: ["gaming", "hiking"],
            location: "SF",
          },
        },
      },
      {
        name: "Charlie Brown",
        age: 35,
        email: "charlie@example.com",
        active: false,
        score: 78.5,
        department: "Marketing",
        tags: ["design", "photoshop"],
        meta: {
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
          notes: "Creative mind",
          details: {
            hobbies: ["painting", "traveling"],
            location: "LA",
          },
        },
      },
      {
        name: "Diana Prince",
        age: 28,
        email: "diana@example.com",
        active: true,
        score: 95.0,
        department: "Engineering",
        tags: ["java", "spring"],
        meta: {
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
          notes: "Java expert",
          details: {
            hobbies: ["yoga", "cooking"],
            location: "Seattle",
          },
        },
      },
      {
        name: "Eve Wilson",
        age: 32,
        email: "eve@example.com",
        active: false,
        score: 88.0,
        department: "Sales",
        tags: ["excel", "powerpoint"],
        meta: {
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
          notes: "Sales strategist",
          details: {
            hobbies: ["running", "photography"],
            location: "Chicago",
          },
        },
      },
      {
        name: "Frank Miller",
        age: 45,
        email: "frank@example.com",
        active: true,
        score: 90.5,
        department: "Engineering",
        tags: ["c++", "algorithms"],
        meta: {
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
          notes: "Senior engineer",
          details: {
            hobbies: ["fishing", "golf"],
            location: "Austin",
          },
        },
      },
      {
        name: "Grace Lee",
        age: 27,
        email: "grace@example.com",
        active: true,
        score: 93.5,
        department: "Marketing",
        tags: ["content", "seo"],
        meta: {
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
          notes: "Content specialist",
          details: {
            hobbies: ["blogging", "cooking"],
            location: "Boston",
          },
        },
      },
      {
        name: "Henry Davis",
        age: 40,
        email: "henry@example.com",
        active: false,
        score: 82.0,
        department: "Sales",
        tags: ["crm", "salesforce"],
        meta: {
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
          notes: "Sales manager",
          details: {
            hobbies: ["cycling", "reading"],
            location: "Miami",
          },
        },
      },
      {
        name: "Ivy Chen",
        age: 29,
        email: "ivy@example.com",
        active: true,
        score: 87.5,
        department: "Engineering",
        tags: ["go", "kubernetes"],
        meta: {
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
          notes: "Cloud engineer",
          details: {
            hobbies: ["swimming", "gaming"],
            location: "Denver",
          },
        },
      },
      {
        name: "Jack Turner",
        age: 33,
        email: "jack@example.com",
        active: true,
        score: 89.0,
        department: "Marketing",
        tags: ["analytics", "tableau"],
        meta: {
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
          notes: "Data analyst",
          details: {
            hobbies: ["traveling", "photography"],
            location: "Portland",
          },
        },
      },
    ];

    testDocuments = await system.createDocuments(
      testCollectionId,
      documentsData.map((data) => new Doc(data)),
    );
  });

  afterAll(async () => {
    await db.delete();
  });

  describe("find", () => {
    test("should find all documents without query", async () => {
      const documents = await system.find(testCollectionId);

      expect(documents).toHaveLength(10);
      documents.forEach((doc) => {
        expect(doc.getId()).toBeDefined();
        expect(doc.get("name")).toBeDefined();
      });
    });

    test("should find documents with equal filter", async () => {
      const documents = await system.find(testCollectionId, [
        Query.equal("department", ["Engineering"]),
      ]);

      expect(documents).toHaveLength(5);
      documents.forEach((doc) => {
        expect(doc.get("department")).toBe("Engineering");
      });
    });

    test("should find documents with multiple values in equal filter", async () => {
      const documents = await system.find(testCollectionId, [
        Query.equal("department", ["Engineering", "Marketing"]),
      ]);

      expect(documents).toHaveLength(8); // 5 Engineering + 3 Marketing
    });

    test("should find documents with not equal filter", async () => {
      const documents = await system.find(testCollectionId, [
        Query.notEqual("department", "Sales"),
      ]);

      expect(documents).toHaveLength(8); // All except 2 Sales
      documents.forEach((doc) => {
        expect(doc.get("department")).not.toBe("Sales");
      });
    });

    test("should find documents with greater than filter", async () => {
      const documents = await system.find(testCollectionId, [
        Query.greaterThan("age", 30),
      ]);

      expect(documents.length).toBeGreaterThan(0);
      documents.forEach((doc) => {
        expect(doc.get("age")).toBeGreaterThan(30);
      });
    });

    test("should find documents with greater than equal filter", async () => {
      const documents = await system.find(testCollectionId, [
        Query.greaterThanEqual("age", 30),
      ]);

      expect(documents.length).toBeGreaterThan(0);
      documents.forEach((doc) => {
        expect(doc.get("age")).toBeGreaterThanOrEqual(30);
      });
    });

    test("should find documents with less than filter", async () => {
      const documents = await system.find(testCollectionId, [
        Query.lessThan("age", 30),
      ]);

      expect(documents.length).toBeGreaterThan(0);
      documents.forEach((doc) => {
        expect(doc.get("age")).toBeLessThan(30);
      });
    });

    test("should find documents with less than equal filter", async () => {
      const documents = await system.find(testCollectionId, [
        Query.lessThanEqual("age", 30),
      ]);

      expect(documents.length).toBeGreaterThan(0);
      documents.forEach((doc) => {
        expect(doc.get("age")).toBeLessThanOrEqual(30);
      });
    });

    test("should find documents with between filter", async () => {
      const documents = await system.find(testCollectionId, [
        Query.between("age", 25, 35),
      ]);

      expect(documents.length).toBeGreaterThan(0);
      documents.forEach((doc) => {
        const age = doc.get("age");
        expect(age).toBeGreaterThanOrEqual(25);
        expect(age).toBeLessThanOrEqual(35);
      });
    });

    test("should find documents with contains filter", async () => {
      const documents = await system.find(testCollectionId, [
        Query.contains("tags", ["javascript"]),
      ]);

      expect(documents.length).toBeGreaterThan(0);
      documents.forEach((doc) => {
        const tags = doc.get("tags") as string[];
        expect(tags).toContain("javascript");
      });
    });

    test("should find documents with json filter", async () => {
      const documents = await system.find(testCollectionId, [
        Query.contains("meta->details->>hobbies", ["traveling"]),
      ]);

      expect(documents.length).toBeGreaterThan(0);
      documents.forEach((doc) => {
        const meta = doc.get("meta");
        expect(meta).toBeDefined();
        expect(meta.details).toBeDefined();
        expect(meta.details.hobbies).toContain("traveling");
      });
    });

    test("should find documents with startsWith filter", async () => {
      const documents = await system.find(testCollectionId, [
        Query.startsWith("name", "A"),
      ]);

      expect(documents.length).toBeGreaterThan(0);
      documents.forEach((doc) => {
        expect(doc.get("name")).toMatch(/^A/);
      });
    });

    test("should find documents with endsWith filter", async () => {
      const documents = await system.find(testCollectionId, [
        Query.endsWith("email", ".com"),
      ]);

      expect(documents).toHaveLength(10); // All emails end with .com
      documents.forEach((doc) => {
        expect(doc.get("email")).toMatch(/\.com$/);
      });
    });

    test("should find documents with isNull filter", async () => {
      // First create a document with null value
      await system.createDocument(
        testCollectionId,
        new Doc({
          name: "Null Test",
          email: null,
        }),
      );

      const documents = await system.find(testCollectionId, [
        Query.isNull("email"),
      ]);

      expect(documents.length).toBeGreaterThan(0);
      documents.forEach((doc) => {
        expect(doc.get("email")).toBeNull();
      });
    });

    test("should find documents with isNotNull filter", async () => {
      const documents = await system.find(testCollectionId, [
        Query.isNotNull("email"),
      ]);

      expect(documents).toHaveLength(10); // All original documents have email
      documents.forEach((doc) => {
        expect(doc.get("email")).not.toBeNull();
      });
    });

    test("should find documents with search filter", async () => {
      const documents = await system.find(testCollectionId, [
        Query.search("name", "Alice"),
      ]);

      expect(documents.length).toBeGreaterThan(0);
    });

    test("should find documents with limit", async () => {
      const documents = await system.find(testCollectionId, [Query.limit(5)]);

      expect(documents).toHaveLength(5);
    });

    test("should find documents with offset", async () => {
      const allDocuments = await system.find(testCollectionId, [
        Query.orderAsc("name"),
      ]);

      const offsetDocuments = await system.find(testCollectionId, [
        Query.orderAsc("name"),
        Query.offset(3),
      ]);

      expect(offsetDocuments).toHaveLength(7); // 10 - 3
      expect(offsetDocuments[0]?.get("name")).toBe(
        allDocuments[3]?.get("name"),
      );
    });

    test("should find documents with orderAsc", async () => {
      const documents = await system.find(testCollectionId, [
        Query.orderAsc("age"),
      ]);

      for (let i = 1; i < documents.length; i++) {
        expect(documents[i]?.get("age")).toBeGreaterThanOrEqual(
          documents[i - 1]?.get("age"),
        );
      }
    });

    test("should find documents with orderDesc", async () => {
      const documents = await system.find(testCollectionId, [
        Query.orderDesc("age"),
      ]);

      for (let i = 1; i < documents.length; i++) {
        expect(documents[i]?.get("age")).toBeLessThanOrEqual(
          documents[i - 1]?.get("age"),
        );
      }
    });

    test("should find documents with select fields", async () => {
      const documents = await system.find(testCollectionId, [
        Query.select(["name", "age"]),
        Query.limit(3),
      ]);

      expect(documents).toHaveLength(3);
      documents.forEach((doc) => {
        expect(doc.get("name")).toBeDefined();
        expect(doc.get("age")).toBeDefined();
        expect(doc.has("email")).toBeFalsy();
        expect(doc.has("department")).toBeFalsy();
      });
    });

    test("should find documents with cursor pagination", async () => {
      const firstBatch = await system.find(testCollectionId, [
        Query.orderAsc("name"),
        Query.limit(5),
      ]);

      expect(firstBatch).toHaveLength(5);

      const secondBatch = await system.find(testCollectionId, [
        Query.orderAsc("name"),
        Query.cursorAfter(firstBatch[4]!),
        Query.limit(5),
      ]);
      expect(secondBatch).toHaveLength(5);

      // Ensure no overlap
      const firstNames = firstBatch.map((doc) => doc.get("name"));
      const secondNames = secondBatch.map((doc) => doc.get("name"));
      const overlap = firstNames.filter((name) => secondNames.includes(name));
      expect(overlap).toHaveLength(0);
    });

    test("should find documents with complex query combinations", async () => {
      const documents = await system.find(testCollectionId, [
        Query.equal("department", ["Engineering"]),
        Query.greaterThan("age", 25),
        Query.lessThan("score", 95),
        Query.orderDesc("score"),
        Query.limit(3),
      ]);

      expect(documents.length).toBeGreaterThan(0);
      expect(documents.length).toBeLessThanOrEqual(3);

      documents.forEach((doc) => {
        expect(doc.get("department")).toBe("Engineering");
        expect(doc.get("age")).toBeGreaterThan(25);
        expect(doc.get("score")).toBeLessThan(95);
      });

      // Check ordering
      for (let i = 1; i < documents.length; i++) {
        expect(documents[i]?.get("score")).toBeLessThanOrEqual(
          documents[i - 1]?.get("score"),
        );
      }
    });

    test("should find documents using QueryBuilder", async () => {
      const documents = await system.find(testCollectionId, (qb) =>
        qb
          .equal("department", "Engineering")
          .greaterThan("age", 25)
          .orderDesc("score")
          .limit(3),
      );

      expect(documents.length).toBeGreaterThan(0);
      expect(documents.length).toBeLessThanOrEqual(3);

      documents.forEach((doc) => {
        expect(doc.get("department")).toBe("Engineering");
        expect(doc.get("age")).toBeGreaterThan(25);
      });
    });

    test("should handle empty result sets", async () => {
      const documents = await system.find(testCollectionId, [
        Query.equal("name", ["Non Existent Person"]),
      ]);

      expect(documents).toHaveLength(0);
    });

    test("should handle boolean filters", async () => {
      const activeDocuments = await system.find(testCollectionId, [
        Query.equal("active", [true]),
      ]);

      const inactiveDocuments = await system.find(testCollectionId, [
        Query.equal("active", [false]),
      ]);

      expect(activeDocuments.length + inactiveDocuments.length).toBe(10);

      activeDocuments.forEach((doc) => {
        expect(doc.get("active")).toBe(true);
      });

      inactiveDocuments.forEach((doc) => {
        expect(doc.get("active")).toBe(false);
      });
    });
  });

  describe("findOne", () => {
    test("should find single document without query", async () => {
      const document = await system.findOne(testCollectionId);

      expect(document.empty()).toBe(false);
      expect(document.getId()).toBeDefined();
    });

    test("should find single document with query", async () => {
      const document = await system.findOne(testCollectionId, [
        Query.equal("department", ["Engineering"]),
        Query.orderAsc("age"),
      ]);

      expect(document.empty()).toBe(false);
      expect(document.get("department")).toBe("Engineering");
    });

    test("should return empty doc when no match found", async () => {
      const document = await system.findOne(testCollectionId, [
        Query.equal("name", ["Non Existent"]),
      ]);

      expect(document.empty()).toBe(true);
    });

    test("should find first document with ordering", async () => {
      const document = await system.findOne(testCollectionId, [
        Query.orderAsc("age"),
      ]);

      expect(document.empty()).toBe(false);

      // Should be the youngest person
      const allDocuments = await system.find(testCollectionId, [
        Query.orderAsc("age"),
      ]);

      expect(document.get("age")).toBe(allDocuments[0]?.get("age"));
    });

    test("should work with QueryBuilder", async () => {
      const document = await system.findOne(testCollectionId, (qb) =>
        qb.equal("active", true).orderDesc("score"),
      );

      expect(document.empty()).toBe(false);
      expect(document.get("active")).toBe(true);
    });
  });

  describe("count", () => {
    test("should count all documents", async () => {
      const count = await system.count(testCollectionId);
      expect(count).toBe(10);
    });

    test("should count with filters", async () => {
      const count = await system.count(testCollectionId, [
        Query.equal("department", ["Engineering"]),
      ]);

      expect(count).toBe(5);
    });

    test("should count with complex filters", async () => {
      const count = await system.count(testCollectionId, [
        Query.equal("active", [true]),
        Query.greaterThan("age", 25),
      ]);

      expect(count).toBeGreaterThan(0);
      expect(count).toBeLessThanOrEqual(10);
    });

    test("should respect max parameter", async () => {
      const count = await system.count(testCollectionId, [], 5);
      expect(count).toBeLessThanOrEqual(5);
    });

    test("should count with QueryBuilder", async () => {
      const count = await system.count(testCollectionId, (qb) =>
        qb.equal("department", "Engineering").equal("active", true),
      );

      expect(count).toBeGreaterThan(0);
    });

    test("should return 0 for no matches", async () => {
      const count = await system.count(testCollectionId, [
        Query.equal("name", ["Non Existent"]),
      ]);

      expect(count).toBe(0);
    });
  });

  describe("sum", () => {
    test("should sum numeric attribute", async () => {
      const sum = await system.sum(testCollectionId, "age");

      expect(sum).toBeGreaterThan(0);
      expect(typeof sum).toBe("number");
    });

    test("should sum with filters", async () => {
      const engineeringSum = await system.sum(testCollectionId, "age", [
        Query.equal("department", ["Engineering"]),
      ]);

      const allSum = await system.sum(testCollectionId, "age");

      expect(engineeringSum).toBeGreaterThan(0);
      expect(engineeringSum).toBeLessThan(allSum);
    });

    test("should sum float values", async () => {
      const sum = await system.sum(testCollectionId, "score");

      expect(sum).toBeGreaterThan(0);
      expect(typeof sum).toBe("number");
    });

    test("should sum with complex filters", async () => {
      const sum = await system.sum(testCollectionId, "score", [
        Query.equal("active", [true]),
        Query.greaterThan("age", 25),
      ]);

      expect(sum).toBeGreaterThan(0);
    });

    test("should respect max parameter", async () => {
      const sumWithMax = await system.sum(testCollectionId, "age", [], 3);
      const sumWithoutMax = await system.sum(testCollectionId, "age");

      expect(sumWithMax).toBeLessThanOrEqual(sumWithoutMax);
    });

    test("should work with QueryBuilder", async () => {
      const sum = await system.sum(testCollectionId, "score", (qb) =>
        qb.equal("department", "Engineering").equal("active", true),
      );

      expect(sum).toBeGreaterThan(0);
    });

    test("should return 0 for no matches", async () => {
      const sum = await system.sum(testCollectionId, "age", [
        Query.equal("name", ["Non Existent"]),
      ]);

      expect(sum).toBe(0);
    });
  });

  describe("edge cases", () => {
    test("should handle queries on non-existent collection", async () => {
      await expect(system.find("non_existent_collection")).rejects.toThrow();
    });

    test("should handle queries with invalid attribute names", async () => {
      await expect(
        system.find(testCollectionId, [
          Query.equal("invalid_attribute", ["value"]),
        ]),
      ).rejects.toThrow();
    });

    test("should handle large result sets efficiently", async () => {
      // Create many documents
      const largeDataSet = Array.from(
        { length: 100 },
        (_, i) =>
          new Doc({
            name: `User ${i}`,
            age: 20 + (i % 50),
            department: i % 2 === 0 ? "Engineering" : "Marketing",
          }),
      );

      await system.createDocuments(testCollectionId, largeDataSet);

      const documents = await system.find(testCollectionId, [Query.limit(50)]);

      expect(documents).toHaveLength(50);
    });

    test("should handle concurrent queries", async () => {
      const queries = Array.from({ length: 5 }, (_, i) =>
        system.find(testCollectionId, [
          Query.equal("department", ["Engineering"]),
          Query.limit(i + 1),
        ]),
      );

      const results = await Promise.all(queries);

      results.forEach((result, index) => {
        expect(result.length).toBeLessThanOrEqual(index + 1);
      });
    });

    test("should handle complex nested queries", async () => {
      const documents = await system.find(testCollectionId, [
        Query.or([
          Query.equal("department", ["Engineering"]),
          Query.greaterThan("age", 30),
        ]),
        Query.or([
          Query.equal("department", ["Marketing"]),
          Query.lessThan("age", 30),
        ]),
        Query.orderAsc("name"),
      ]);

      expect(documents.length).toBeGreaterThan(0);
    });
  });
});
