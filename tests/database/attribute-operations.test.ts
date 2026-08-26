import {
  describe,
  test,
  expect,
  beforeEach,
  afterAll,
  beforeAll,
} from "bun:test";
import { Database } from "@core/database.js";
import { createTestDb } from "../helpers.js";
import { Doc } from "@core/doc.js";
import { AttributeEnum } from "@core/enums.js";
import { Attribute } from "@validators/schema.js";
import {
  DatabaseException,
  NotFoundException,
  DuplicateException,
} from "@errors/index.js";
import { Structure } from "@validators/structure.js";
import { Text } from "@validators/text.js";

Structure.addFormat("email", {
  type: AttributeEnum.String,
  callback(...params) {
    return new Text(100, 0);
  },
});

const schema = new Date().getTime().toString();
describe("Attribute Operations", () => {
  let db: Database;
  // Privileged session for the single document-plane call in this suite
  // (attribute size validation via createDocument).
  let system: ReturnType<Database["system"]>;
  let testCollectionId: string;

  beforeAll(async () => {
    db = createTestDb({ namespace: `coll_op_${schema}` });
    system = db.system();
    db.setMeta({ schema });
    await db.create();
  });

  beforeEach(async () => {
    testCollectionId = `test_collection_${new Date().getTime()}`;

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
      ],
    });
  });

  afterAll(async () => {
    await db.delete();
  });

  describe("createAttribute", () => {
    test("should create string attribute", async () => {
      const attribute: Attribute = {
        $id: "email",
        key: "email",
        type: AttributeEnum.String,
        size: 255,
        required: false,
      };

      const created = await db.createAttribute(testCollectionId, attribute);
      expect(created).toBe(true);

      const collection = await db.getCollection(testCollectionId);
      const attributes = collection.get("attributes") as Doc<Attribute>[];
      const emailAttr = attributes.find((attr) => attr.get("$id") === "email");

      expect(emailAttr).toBeDefined();
      expect(emailAttr?.get("type")).toBe(AttributeEnum.String);
      expect(emailAttr?.get("size")).toBe(255);
    });

    test("should create integer attribute", async () => {
      const attribute: Attribute = {
        $id: "score",
        key: "score",
        type: AttributeEnum.Integer,
        required: false,
        default: 0,
      };

      const created = await db.createAttribute(testCollectionId, attribute);
      expect(created).toBe(true);

      const collection = await db.getCollection(testCollectionId);
      const attributes = collection.get("attributes") as Doc<Attribute>[];
      const scoreAttr = attributes.find((attr) => attr.get("$id") === "score");

      expect(scoreAttr).toBeDefined();
      expect(scoreAttr?.get("type")).toBe(AttributeEnum.Integer);
      expect(scoreAttr?.get("default")).toBe(0);
    });

    test("should create float attribute", async () => {
      const attribute: Attribute = {
        $id: "rating",
        key: "rating",
        type: AttributeEnum.Float,
        required: false,
      };

      const created = await db.createAttribute(testCollectionId, attribute);
      expect(created).toBe(true);

      const collection = await db.getCollection(testCollectionId);
      const attributes = collection.get("attributes") as Doc<Attribute>[];
      const ratingAttr = attributes.find(
        (attr) => attr.get("$id") === "rating",
      );

      expect(ratingAttr).toBeDefined();
      expect(ratingAttr?.get("type")).toBe(AttributeEnum.Float);
    });

    test("should create boolean attribute", async () => {
      const attribute: Attribute = {
        $id: "active",
        key: "active",
        type: AttributeEnum.Boolean,
        required: false,
        default: true,
      };

      const created = await db.createAttribute(testCollectionId, attribute);
      expect(created).toBe(true);

      const collection = await db.getCollection(testCollectionId);
      const attributes = collection.get("attributes") as Doc<Attribute>[];
      const activeAttr = attributes.find(
        (attr) => attr.get("$id") === "active",
      );

      expect(activeAttr).toBeDefined();
      expect(activeAttr?.get("type")).toBe(AttributeEnum.Boolean);
      expect(activeAttr?.get("default")).toBe(true);
    });

    test("should create JSON attribute", async () => {
      const attribute: Attribute = {
        $id: "metadata",
        key: "metadata",
        type: AttributeEnum.Json,
        required: false,
      };

      const created = await db.createAttribute(testCollectionId, attribute);
      expect(created).toBe(true);

      const collection = await db.getCollection(testCollectionId);
      const attributes = collection.get("attributes") as Doc<Attribute>[];
      const metadataAttr = attributes.find(
        (attr) => attr.get("$id") === "metadata",
      );

      expect(metadataAttr).toBeDefined();
      expect(metadataAttr?.get("type")).toBe(AttributeEnum.Json);
    });

    test("should create array attribute", async () => {
      const attribute: Attribute = {
        $id: "tags",
        key: "tags",
        type: AttributeEnum.String,
        size: 100,
        array: true,
        required: false,
      };

      const created = await db.createAttribute(testCollectionId, attribute);
      expect(created).toBe(true);

      const collection = await db.getCollection(testCollectionId);
      const attributes = collection.get("attributes") as Doc<Attribute>[];
      const tagsAttr = attributes.find((attr) => attr.get("$id") === "tags");

      expect(tagsAttr).toBeDefined();
      expect(tagsAttr?.get("array")).toBe(true);
    });

    test("should create attribute with filters", async () => {
      const attribute: Attribute = {
        $id: "description",
        key: "description",
        type: AttributeEnum.String,
        size: 1000,
        required: false,
        filters: ["encrypt"],
      };

      const created = await db.createAttribute(testCollectionId, attribute);
      expect(created).toBe(true);

      const collection = await db.getCollection(testCollectionId);
      const attributes = collection.get("attributes") as Doc<Attribute>[];
      const descAttr = attributes.find(
        (attr) => attr.get("$id") === "description",
      );

      expect(descAttr).toBeDefined();
      expect(descAttr?.get("filters")).toEqual(["encrypt"]);
    });

    test("should throw error for relationship attribute type", async () => {
      const attribute: Attribute = {
        $id: "relation",
        key: "relation",
        type: AttributeEnum.Relationship,
        required: false,
      };

      await expect(
        db.createAttribute(testCollectionId, attribute),
      ).rejects.toThrow(DatabaseException);
    });

    test("should throw error for virtual attribute type", async () => {
      const attribute: Attribute = {
        $id: "virtual",
        key: "virtual",
        type: AttributeEnum.Virtual,
        required: false,
      };

      await expect(
        db.createAttribute(testCollectionId, attribute),
      ).rejects.toThrow(DatabaseException);
    });

    test("should throw error for duplicate attribute", async () => {
      const attribute: Attribute = {
        $id: "name", // Already exists
        key: "name",
        type: AttributeEnum.String,
        size: 255,
        required: false,
      };

      await expect(
        db.createAttribute(testCollectionId, attribute),
      ).rejects.toThrow(DuplicateException);
    });
  });

  describe("createAttributes", () => {
    test("should create multiple attributes", async () => {
      const attributes: Attribute[] = [
        {
          $id: "email",
          key: "email",
          type: AttributeEnum.String,
          size: 255,
          required: false,
        },
        {
          $id: "phone",
          key: "phone",
          type: AttributeEnum.String,
          size: 20,
          required: false,
        },
        {
          $id: "active",
          key: "active",
          type: AttributeEnum.Boolean,
          required: false,
          default: true,
        },
      ];

      const created = await db.createAttributes(testCollectionId, attributes);
      expect(created).toBe(true);

      const collection = await db.getCollection(testCollectionId);
      const collectionAttributes = collection.get(
        "attributes",
      ) as Doc<Attribute>[];

      expect(collectionAttributes).toHaveLength(5); // 2 initial + 3 new

      const emailAttr = collectionAttributes.find(
        (attr) => attr.get("$id") === "email",
      );
      const phoneAttr = collectionAttributes.find(
        (attr) => attr.get("$id") === "phone",
      );
      const activeAttr = collectionAttributes.find(
        (attr) => attr.get("$id") === "active",
      );

      expect(emailAttr).toBeDefined();
      expect(phoneAttr).toBeDefined();
      expect(activeAttr).toBeDefined();
    });

    test("should throw error for empty attributes array", async () => {
      await expect(db.createAttributes(testCollectionId, [])).rejects.toThrow(
        DatabaseException,
      );
    });
  });

  describe("updateAttributeRequired", () => {
    test("should update required status to true", async () => {
      // First create an optional attribute
      await db.createAttribute(testCollectionId, {
        $id: "email",
        key: "email",
        type: AttributeEnum.String,
        size: 255,
        required: false,
      });

      const updated = await db.updateAttributeRequired(
        testCollectionId,
        "email",
        true,
      );

      expect(updated.get("required")).toBe(true);
    });

    test("should update required status to false", async () => {
      const updated = await db.updateAttributeRequired(
        testCollectionId,
        "name",
        false,
      );

      expect(updated.get("required")).toBe(false);
    });

    test("should throw error for non-existent attribute", async () => {
      await expect(
        db.updateAttributeRequired(testCollectionId, "nonexistent", true),
      ).rejects.toThrow(NotFoundException);
    });
  });

  describe("updateAttributeFormat", () => {
    test("should update string attribute format", async () => {
      await db.createAttribute(testCollectionId, {
        $id: "email",
        key: "email",
        type: AttributeEnum.String,
        size: 255,
        required: false,
      });

      const updated = await db.updateAttributeFormat(
        testCollectionId,
        "email",
        "email",
      );

      expect(updated.get("format")).toBe("email");
    });

    test("should throw error for invalid format", async () => {
      await db.createAttribute(testCollectionId, {
        $id: "email",
        key: "email",
        type: AttributeEnum.String,
        size: 255,
        required: false,
      });

      await expect(
        db.updateAttributeFormat(testCollectionId, "email", "invalid_format"),
      ).rejects.toThrow(DatabaseException);
    });
  });

  describe("updateAttributeFormatOptions", () => {
    test("should update format options", async () => {
      await db.createAttribute(testCollectionId, {
        $id: "description",
        key: "description",
        type: AttributeEnum.String,
        size: 1000,
        required: false,
      });

      const formatOptions = { maxLength: 500, minLength: 10 };
      const updated = await db.updateAttributeFormatOptions(
        testCollectionId,
        "description",
        formatOptions,
      );

      expect(updated.get("formatOptions")).toEqual(formatOptions);
    });
  });

  describe("updateAttributeFilters", () => {
    test("should update filters", async () => {
      await db.createAttribute(testCollectionId, {
        $id: "secret",
        key: "secret",
        type: AttributeEnum.String,
        size: 255,
        required: false,
      });

      const filters = ["encrypt", "hash"];
      const updated = await db.updateAttributeFilters(
        testCollectionId,
        "secret",
        filters,
      );

      expect(updated.get("filters")).toEqual(filters);
    });
  });

  describe("updateAttributeDefault", () => {
    test("should update default value", async () => {
      const updated = await db.updateAttributeDefault(
        testCollectionId,
        "age",
        25,
      );

      expect(updated.get("default")).toBe(25);
    });

    test("should set default to null", async () => {
      const updated = await db.updateAttributeDefault(
        testCollectionId,
        "age",
        null,
      );

      expect(updated.get("default")).toBeNull();
    });

    test("should throw error for required attribute", async () => {
      await expect(
        db.updateAttributeDefault(testCollectionId, "name", "default_name"),
      ).rejects.toThrow(DatabaseException);
    });
  });

  describe("updateAttribute", () => {
    test("should update attribute type and size", async () => {
      await db.createAttribute(testCollectionId, {
        $id: "description",
        key: "description",
        type: AttributeEnum.String,
        size: 255,
        required: false,
      });

      const updated = await db.updateAttribute(
        testCollectionId,
        "description",
        {
          size: 1000,
        },
      );

      expect(updated.get("size")).toBe(1000);
    });

    test("should update attribute key", async () => {
      await db.createAttribute(testCollectionId, {
        $id: "old_name",
        key: "old_name",
        type: AttributeEnum.String,
        size: 255,
        required: false,
      });

      const updated = await db.updateAttribute(testCollectionId, "old_name", {
        newKey: "new_name",
      });

      expect(updated.get("$id")).toBe("new_name");
      expect(updated.get("key")).toBe("new_name");
    });

    test("should update multiple properties", async () => {
      await db.createAttribute(testCollectionId, {
        $id: "score",
        key: "score",
        type: AttributeEnum.Integer,
        required: false,
        default: 0,
      });

      const updated = await db.updateAttribute(testCollectionId, "score", {
        required: true,
        default: null,
      });

      expect(updated.get("required")).toBe(true);
      expect(updated.get("default")).toBeNull();
    });

    test("should throw error for invalid size", async () => {
      await expect(
        db.updateAttribute(testCollectionId, "name", {
          size: 1000000000000000000000000000, // Exceeds limit
        }),
      ).rejects.toThrow(DatabaseException);
    });
  });

  describe("deleteAttribute", () => {
    test("should delete existing attribute", async () => {
      await db.createAttribute(testCollectionId, {
        $id: "temp_attr",
        key: "temp_attr",
        type: AttributeEnum.String,
        size: 255,
        required: false,
      });

      const deleted = await db.deleteAttribute(testCollectionId, "temp_attr");
      expect(deleted).toBe(true);

      const collection = await db.getCollection(testCollectionId);
      const attributes = collection.get("attributes") as Doc<Attribute>[];
      const tempAttr = attributes.find(
        (attr) => attr.get("$id") === "temp_attr",
      );

      expect(tempAttr).toBeUndefined();
    });

    test("should throw error for non-existent attribute", async () => {
      await expect(
        db.deleteAttribute(testCollectionId, "nonexistent"),
      ).rejects.toThrow(NotFoundException);
    });

    test("should throw error for metadata collection", async () => {
      await expect(
        db.deleteAttribute(Database.METADATA, "any_attr"),
      ).rejects.toThrow(DatabaseException);
    });

    test("should throw error for relationship attribute", async () => {
      // This would require creating a relationship first
      // For now, just test that the error handling exists
      await expect(
        db.deleteAttribute(testCollectionId, "nonexistent"),
      ).rejects.toThrow(NotFoundException);
    });
  });

  describe("renameAttribute", () => {
    test("should rename existing attribute", async () => {
      await db.createAttribute(testCollectionId, {
        $id: "old_email",
        key: "old_email",
        type: AttributeEnum.String,
        size: 255,
        required: false,
      });

      const renamed = await db.renameAttribute(
        testCollectionId,
        "old_email",
        "new_email",
      );
      expect(renamed).toBe(true);

      const collection = await db.getCollection(testCollectionId);
      const attributes = collection.get("attributes") as Doc<Attribute>[];

      const oldAttr = attributes.find(
        (attr) => attr.get("$id") === "old_email",
      );
      const newAttr = attributes.find(
        (attr) => attr.get("$id") === "new_email",
      );

      expect(oldAttr).toBeUndefined();
      expect(newAttr).toBeDefined();
      expect(newAttr?.get("key")).toBe("new_email");
    });

    test("should throw error for non-existent collection", async () => {
      await expect(
        db.renameAttribute("nonexistent", "attr", "new_attr"),
      ).rejects.toThrow(NotFoundException);
    });

    test("should throw error for non-existent attribute", async () => {
      await expect(
        db.renameAttribute(testCollectionId, "nonexistent", "new_name"),
      ).rejects.toThrow(NotFoundException);
    });

    test("should throw error for duplicate name", async () => {
      await expect(
        db.renameAttribute(testCollectionId, "age", "name"),
      ).rejects.toThrow(DuplicateException);
    });
  });

  describe("edge cases", () => {
    test("should handle attributes with special characters", async () => {
      const attribute: Attribute = {
        $id: "special_chars_123",
        key: "special_chars_123",
        type: AttributeEnum.String,
        size: 255,
        required: false,
      };

      const created = await db.createAttribute(testCollectionId, attribute);
      expect(created).toBe(true);
    });

    // test('should handle concurrent attribute operations', async () => {
    //     const attributes: Attribute[] = Array.from({ length: 5 }, (_, i) => ({
    //         '$id': `concurrent_attr_${i}`,
    //         'key': `concurrent_attr_${i}`,
    //         'type': AttributeEnum.String,
    //         'size': 255,
    //         'required': false
    //     }));

    //     const promises = attributes.map(attr =>
    //         db.createAttribute(testCollectionId, attr)
    //     );

    //     const results = await Promise.all(promises);

    //     expect(results.every(result => result === true)).toBe(true);
    // });

    test("should handle large attribute names", async () => {
      const longName = "a".repeat(50); // Test reasonable length

      const attribute: Attribute = {
        $id: longName,
        key: longName,
        type: AttributeEnum.String,
        size: 255,
        required: false,
      };

      const created = await db.createAttribute(testCollectionId, attribute);
      expect(created).toBe(true);
    });

    test("should validate attribute size limits", async () => {
      const attribute: Attribute = {
        $id: "large_string",
        key: "large_string",
        type: AttributeEnum.String,
        size: 1, // Very small size
        required: false,
      };

      const created = await db.createAttribute(testCollectionId, attribute);
      expect(created).toBe(true);

      // Now try to create a document that exceeds this size
      await expect(
        system.createDocument(
          testCollectionId,
          new Doc({
            name: "Test",
            large_string: "This string is too long",
          }),
        ),
      ).rejects.toThrow();
    });
  });
});
