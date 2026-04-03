import { describe, test, expect, beforeEach } from "vitest";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";
import { Doc, DocException, IEntity } from "index.js";

describe("Doc Class", () => {
  describe("Constructor", () => {
    test("should create an empty Doc when no data is provided", () => {
      const doc = new Doc();
      expect(doc.empty()).toBe(true);
      expect(doc.keys().length).toBe(0);
    });

    test("should create Doc with primitive values", () => {
      const doc = new Doc({
        $id: "123",
        name: "Test",
        age: 25,
        active: true,
      });

      expect(doc.get("$id")).toBe("123");
      expect(doc.get("name")).toBe("Test");
      expect(doc.get("age")).toBe(25);
      expect(doc.get("active")).toBe(true);
    });

    test("should transform entity-like objects into Doc instances", () => {
      const doc = new Doc({
        $id: "parent",
        child: { $id: "child", name: "Child" },
      });

      const child = doc.get("child");
      expect(child).toBeInstanceOf(Doc);
      expect(child.get("$id")).toBe("child");
      expect(child.get("name")).toBe("Child");
    });

    test("should transform arrays of entity-like objects into Doc instances", () => {
      const doc = new Doc({
        $id: "parent",
        children: [
          { $id: "child1", name: "Child 1" },
          { $id: "child2", name: "Child 2" },
        ],
      });

      const children = doc.get("children");
      expect(Array.isArray(children)).toBe(true);
      expect(children[0]).toBeInstanceOf(Doc);
      expect(children[0]?.get("name")).toBe("Child 1");
      expect(children[1]).toBeInstanceOf(Doc);
      expect(children[1]?.get("name")).toBe("Child 2");
    });

    test("should preserve existing Doc instances in arrays", () => {
      const existingDoc = new Doc({ $id: "existing", name: "Existing" });
      const doc = new Doc({
        $id: "parent",
        items: [existingDoc, { $id: "new", name: "New" }],
      });

      const items = doc.get("items");
      expect(items[0]).toBe(existingDoc);
      expect(items[1]).toBeInstanceOf(Doc);
    });

    test("should throw DocException when $id is not a string", () => {
      expect(() => {
        new Doc({ $id: 123 as any });
      }).toThrow(DocException);
      expect(() => {
        new Doc({ $id: 123 as any });
      }).toThrow("$id must be a string");
    });

    test("should throw DocException when $permissions is not an array", () => {
      expect(() => {
        new Doc({ $id: "test", $permissions: "invalid" as any });
      }).toThrow(DocException);
      expect(() => {
        new Doc({ $id: "test", $permissions: {} as any });
      }).toThrow("$permissions must be an array");
    });

    test("should convert null and undefined values to null", () => {
      const doc = new Doc({
        $id: "test",
        nullValue: null,
        undefinedValue: undefined,
      });

      expect(doc.get("nullValue")).toBe(null);
      expect(doc.get("undefinedValue")).toBe(null);
    });
  });

  describe("Static Methods", () => {
    test("Doc.from() should create a new Doc instance", () => {
      const doc = Doc.from<{ name: string } & IEntity>({
        $id: "test",
        name: "Test Name",
      });

      expect(doc).toBeInstanceOf(Doc);
      expect(doc.get("$id")).toBe("test");
      expect(doc.get("name")).toBe("Test Name");
    });
  });

  describe("Get Operations", () => {
    let doc: Doc<any>;

    beforeEach(() => {
      doc = new Doc({
        $id: "test",
        name: "Test",
        count: 0,
        active: false,
      });
    });

    test("should get existing field value", () => {
      expect(doc.get("name")).toBe("Test");
      expect(doc.get("count")).toBe(0);
      expect(doc.get("active")).toBe(false);
    });

    test("should return null for non-existing field by default", () => {
      expect(doc.get("nonexistent")).toBe(null);
    });

    test("should return provided default value for non-existing field", () => {
      expect(doc.get("nonexistent", "default")).toBe("default");
      expect(doc.get("nonexistent", 42)).toBe(42);
    });

    test("should return actual value over default for existing field", () => {
      expect(doc.get("count", 999)).toBe(0);
      expect(doc.get("active", true)).toBe(false);
    });

    test("getAll() should return shallow copy of all fields", () => {
      const all = doc.getAll();

      expect(all).toEqual({
        $id: "test",
        name: "Test",
        count: 0,
        active: false,
      });

      // Verify it's a copy
      all["name"] = "Modified";
      expect(doc.get("name")).toBe("Test");
    });
  });

  describe("Set Operations", () => {
    let doc: Doc;

    beforeEach(() => {
      doc = new Doc({ $id: "test" });
    });

    test("should set primitive values", () => {
      doc.set("name", "New Name");
      doc.set("age", 30);
      doc.set("active", true);

      expect(doc.get("name")).toBe("New Name");
      expect(doc.get("age")).toBe(30);
      expect(doc.get("active")).toBe(true);
    });

    test("should transform entity-like objects into Doc", () => {
      doc.set("child", { $id: "child", name: "Child" });

      const child = doc.get("child", new Doc());
      expect(child).toBeInstanceOf(Doc);
      expect(child?.get("$id")).toBe("child");
    });

    test("should preserve existing Doc instances", () => {
      const existingDoc = new Doc({ $id: "existing" });
      doc.set("reference", existingDoc);

      expect(doc.get("reference")).toBe(existingDoc);
    });

    test("should transform arrays with entity-like objects", () => {
      doc.set("items", [
        { $id: "item1" },
        "string",
        123,
        { $collection: "test" },
      ]);

      const items = doc.get("items");
      expect(items?.[0]).toBeInstanceOf(Doc);
      expect(items?.[1]).toBe("string");
      expect(items?.[2]).toBe(123);
      expect(items?.[3]).toBeInstanceOf(Doc);
    });

    test("should convert null/undefined to null", () => {
      doc.set("nullField", null);
      doc.set("undefinedField", undefined);

      expect(doc.get("nullField")).toBe(null);
      expect(doc.get("undefinedField")).toBe(null);
    });

    test("should return this for method chaining", () => {
      const result = doc.set("name", "Test");
      expect(result).toBe(doc);
    });
  });

  describe("Update Operations", () => {
    let doc: Doc<any>;

    beforeEach(() => {
      doc = new Doc({ $id: "test", name: "Original", age: 25 });
    });

    test("update() should update field when value is provided", () => {
      doc.update("name", "Updated");
      expect(doc.get("name")).toBe("Updated");
    });

    test("update() should not update field when value is undefined", () => {
      doc.update("name", undefined);
      expect(doc.get("name")).toBe("Original");
    });

    test("updateAll() should update only defined fields", () => {
      doc.updateAll({
        name: "New Name",
        age: undefined,
        newField: "New Value",
      });

      expect(doc.get("name")).toBe("New Name");
      expect(doc.get("age")).toBe(25);
      expect(doc.get("newField")).toBe("New Value");
    });

    test("setAll() should set all fields including null", () => {
      doc.setAll({
        name: "New Name",
        age: null,
        active: true,
      });

      expect(doc.get("name")).toBe("New Name");
      expect(doc.get("age")).toBe(null);
      expect(doc.get("active")).toBe(true);
    });
  });

  describe("Array Operations", () => {
    let doc: Doc<any>;

    beforeEach(() => {
      doc = new Doc({
        $id: "test",
        tags: ["tag1", "tag2"],
        items: [],
      });
    });

    test("append() should add value to end of array", () => {
      doc.append("tags", "tag3");
      expect(doc.get("tags")).toEqual(["tag1", "tag2", "tag3"]);
    });

    test("append() should transform entity-like objects into Doc", () => {
      doc.append("items", { $id: "item1", name: "Item 1" });
      const items = doc.get("items");

      expect(items[0]).toBeInstanceOf(Doc);
      expect(items[0].get("$id")).toBe("item1");
    });

    test("append() should throw when field is not an array", () => {
      doc.set("notArray", "string");

      expect(() => {
        doc.append("notArray", "value");
      }).toThrow(DocException);
      expect(() => {
        doc.append("notArray", "value");
      }).toThrow("Cannot append to notArray, it is not an array");
    });

    test("prepend() should add value to start of array", () => {
      doc.prepend("tags", "tag0");
      expect(doc.get("tags")).toEqual(["tag0", "tag1", "tag2"]);
    });

    test("prepend() should transform entity-like objects into Doc", () => {
      doc.prepend("items", { $id: "item0", name: "Item 0" });
      const items = doc.get("items");

      expect(items[0]).toBeInstanceOf(Doc);
      expect(items[0].get("$id")).toBe("item0");
    });

    test("prepend() should throw when field is not an array", () => {
      doc.set("notArray", "string");

      expect(() => {
        doc.prepend("notArray", "value");
      }).toThrow(DocException);
      expect(() => {
        doc.prepend("notArray", "value");
      }).toThrow("Cannot prepend to notArray, it is not an array");
    });
  });

  describe("Field Management", () => {
    let doc: Doc<any>;

    beforeEach(() => {
      doc = new Doc({
        $id: "test",
        name: "Test",
        age: 25,
        tags: ["tag1"],
      });
    });

    test("delete() should remove existing field", () => {
      doc.delete("age");
      expect(doc.has("age")).toBe(false);
      expect(doc.get("age")).toBe(null);
    });

    test("delete() should do nothing for non-existing field", () => {
      doc.delete("nonexistent");
      expect(doc.has("nonexistent")).toBe(false);
    });

    test("has() should return true for existing fields", () => {
      expect(doc.has("$id")).toBe(true);
      expect(doc.has("name")).toBe(true);
      expect(doc.has("tags")).toBe(true);
    });

    test("has() should return false for non-existing fields", () => {
      expect(doc.has("nonexistent")).toBe(false);
      expect(doc.has("missing")).toBe(false);
    });

    test("keys() should return array of field names", () => {
      const keys = doc.keys();
      expect(keys).toContain("$id");
      expect(keys).toContain("name");
      expect(keys).toContain("age");
      expect(keys).toContain("tags");
      expect(keys.length).toBe(4);
    });

    test("empty() should return true for empty document", () => {
      const emptyDoc = new Doc();
      expect(emptyDoc.empty()).toBe(true);
    });

    test("empty() should return false for non-empty document", () => {
      expect(doc.empty()).toBe(false);
    });
  });

  describe("Entity-Specific Getters", () => {
    test("getId() should return document ID", () => {
      const doc = new Doc({ $id: "doc123" });
      expect(doc.getId()).toBe("doc123");
    });

    test("getSequence() should return sequence number", () => {
      const doc = new Doc({ $id: "test", $sequence: 42 });
      expect(doc.getSequence()).toBe(42);
    });

    test("getTenant() should return tenant ID", () => {
      const doc = new Doc({ $id: "test", $tenant: 5 });
      expect(doc.getTenant()).toBe(5);
    });

    test("getTenant() should return null when not set", () => {
      const doc = new Doc({ $id: "test" });
      expect(doc.getTenant()).toBe(null);
    });

    test("getTenant() should throw when value is invalid", () => {
      const doc = new Doc({ $id: "test", $tenant: "invalid" as any });
      expect(() => doc.getTenant()).toThrow(DocException);
      expect(() => doc.getTenant()).toThrow("$tenant must be a number or null");
    });

    test("getCollection() should return collection name", () => {
      const doc = new Doc({ $id: "test", $collection: "users" });
      expect(doc.getCollection()).toBe("users");
    });

    test("createdAt() should return Date object", () => {
      const now = new Date();
      const doc = new Doc({ $id: "test", $createdAt: now });
      expect(doc.createdAt()).toEqual(now);
    });

    test("createdAt() should convert string to Date", () => {
      const dateString = "2024-01-01T00:00:00.000Z";
      const doc = new Doc({ $id: "test", $createdAt: dateString });
      const result = doc.createdAt();

      expect(result).toBeInstanceOf(Date);
      expect(result?.toISOString()).toBe(dateString);
    });

    test("createdAt() should return null when not set", () => {
      const doc = new Doc({ $id: "test" });
      expect(doc.createdAt()).toBe(null);
    });

    test("updatedAt() should return Date object", () => {
      const now = new Date();
      const doc = new Doc({ $id: "test", $updatedAt: now });
      expect(doc.updatedAt()).toEqual(now);
    });

    test("updatedAt() should convert string to Date", () => {
      const dateString = "2024-01-01T12:00:00.000Z";
      const doc = new Doc({ $id: "test", $updatedAt: dateString });
      const result = doc.updatedAt();

      expect(result).toBeInstanceOf(Date);
      expect(result?.toISOString()).toBe(dateString);
    });

    test("updatedAt() should return null when not set", () => {
      const doc = new Doc({ $id: "test" });
      expect(doc.updatedAt()).toBe(null);
    });
  });

  describe("Permission Methods", () => {
    test("getPermissions() should return array of permission strings", () => {
      const doc = new Doc({
        $id: "test",
        $permissions: ["read(any)", "write(user:123)"],
      });

      expect(doc.getPermissions()).toEqual(["read(any)", "write(user:123)"]);
    });

    test("getPermissions() should convert Permission objects to strings", () => {
      const doc = new Doc({
        $id: "test",
        $permissions: [Permission.read(Role.any()), "write(user:123)"],
      });

      const permissions = doc.getPermissions();
      expect(permissions).toContain('read("any")');
      expect(permissions).toContain("write(user:123)");
    });

    test("getPermissions() should deduplicate permissions", () => {
      const doc = new Doc({
        $id: "test",
        $permissions: [
          "read(any)",
          "read(any)",
          "write(user:123)",
          "write(user:123)",
        ],
      });

      const permissions = doc.getPermissions();
      expect(permissions.length).toBe(2);
      expect(permissions).toContain("read(any)");
      expect(permissions).toContain("write(user:123)");
    });

    test("getPermissions() should filter out falsy values", () => {
      const doc = new Doc({
        $id: "test",
        $permissions: ["read(any)", null, "", undefined, "write(user:123)"],
      } as any);

      const permissions = doc.getPermissions();
      expect(permissions.length).toBe(2);
      expect(permissions).toEqual(["read(any)", "write(user:123)"]);
    });

    test("getRead() should return read permissions", () => {
      const doc = new Doc({
        $id: "test",
        $permissions: [
          "read(any)",
          "read(user:123)",
          "write(user:456)",
          "create(user:789)",
        ],
      });

      const readPerms = doc.getRead();
      expect(readPerms).toEqual(["any", "user:123"]);
    });

    test("getCreate() should return create permissions", () => {
      const doc = new Doc({
        $id: "test",
        $permissions: ["create(any)", "create(user:123)", "read(user:456)"],
      });

      const createPerms = doc.getCreate();
      expect(createPerms).toEqual(["any", "user:123"]);
    });

    test("getUpdate() should return update permissions", () => {
      const doc = new Doc({
        $id: "test",
        $permissions: ["update(any)", "update(user:123)", "read(user:456)"],
      });

      const updatePerms = doc.getUpdate();
      expect(updatePerms).toEqual(["any", "user:123"]);
    });

    test("getDelete() should return delete permissions", () => {
      const doc = new Doc({
        $id: "test",
        $permissions: ["delete(any)", "delete(user:123)", "read(user:456)"],
      });

      const deletePerms = doc.getDelete();
      expect(deletePerms).toEqual(["any", "user:123"]);
    });

    test("getWrite() should return combined create, update, delete permissions", () => {
      const doc = new Doc({
        $id: "test",
        $permissions: [
          "create(user:123)",
          "update(user:456)",
          "delete(user:789)",
          "update(user:123)",
          "read(any)",
        ],
      });

      const writePerms = doc.getWrite();
      expect(writePerms).toContain("user:123");
      expect(writePerms).toContain("user:456");
      expect(writePerms).toContain("user:789");
      expect(writePerms).not.toContain("any");
    });

    test("getPermissionsByType() should extract permissions by type", () => {
      const doc = new Doc({
        $id: "test",
        $permissions: ["read(any)", "read(user:123)", "write(user:456)"],
      });

      expect(doc.getPermissionsByType("read")).toEqual(["any", "user:123"]);
      expect(doc.getPermissionsByType("write")).toEqual(["user:456"]);
    });
  });

  describe("Query Methods", () => {
    describe("findWhere()", () => {
      test("should find item in array matching predicate", () => {
        const doc = new Doc({
          $id: "test",
          items: [
            { id: 1, name: "Item 1" },
            { id: 2, name: "Item 2" },
            { id: 3, name: "Item 3" },
          ],
        });

        const found = doc.findWhere("items", (item: any) => item.id === 2);
        expect(found).toEqual({ id: 2, name: "Item 2" });
      });

      test("should return null when no match in array", () => {
        const doc = new Doc({
          $id: "test",
          items: [{ id: 1 }, { id: 2 }],
        });

        const found = doc.findWhere("items", (item: any) => item.id === 999);
        expect(found).toBe(null);
      });

      test("should check single value against predicate", () => {
        const doc = new Doc({
          $id: "test",
          status: "active",
        });

        const found = doc.findWhere("status", (val: any) => val === "active");
        expect(found).toBe("active");
      });

      test("should return null when single value doesn't match", () => {
        const doc = new Doc({
          $id: "test",
          status: "inactive",
        });

        const found = doc.findWhere("status", (val: any) => val === "active");
        expect(found).toBe(null);
      });
    });

    describe("replaceWhere()", () => {
      test("should replace matching items in array with value", () => {
        const doc = new Doc({
          $id: "test",
          numbers: [1, 2, 3, 2, 4],
        });

        doc.replaceWhere("numbers", (n: number) => n === 2, 99);
        expect(doc.get("numbers")).toEqual([1, 99, 3, 99, 4]);
      });

      test("should replace matching items in array with function result", () => {
        const doc = new Doc({
          $id: "test",
          numbers: [1, 2, 3, 4],
        });

        doc.replaceWhere(
          "numbers",
          (n: number) => n % 2 === 0,
          (n) => n * 10,
        );
        expect(doc.get("numbers")).toEqual([1, 20, 3, 40]);
      });

      test("should replace single value when it matches", () => {
        const doc = new Doc({
          $id: "test",
          status: "pending",
        });

        doc.replaceWhere(
          "status",
          (val: string) => val === "pending",
          "active",
        );
        expect(doc.get("status")).toBe("active");
      });

      test("should not replace when no match", () => {
        const doc = new Doc({
          $id: "test",
          numbers: [1, 2, 3],
        });

        doc.replaceWhere("numbers", (n: number) => n === 99, 100);
        expect(doc.get("numbers")).toEqual([1, 2, 3]);
      });
    });

    describe("deleteWhere()", () => {
      test("should filter out matching items from array", () => {
        const doc = new Doc({
          $id: "test",
          numbers: [1, 2, 3, 2, 4],
        });

        doc.deleteWhere("numbers", (n: number) => n === 2);
        expect(doc.get("numbers")).toEqual([1, 3, 4]);
      });

      test("should delete field when single value matches", () => {
        const doc = new Doc({
          $id: "test",
          status: "inactive",
        });

        doc.deleteWhere("status", (val: string) => val === "inactive");
        expect(doc.has("status")).toBe(false);
      });

      test("should not delete when no match", () => {
        const doc = new Doc({
          $id: "test",
          status: "active",
        });

        doc.deleteWhere("status", (val: string) => val === "inactive");
        expect(doc.has("status")).toBe(true);
        expect(doc.get("status")).toBe("active");
      });
    });
  });

  describe("Serialization", () => {
    describe("toObject()", () => {
      test("should convert to plain object", () => {
        const doc = new Doc({
          $id: "test",
          name: "Test",
          age: 25,
        });

        const obj = doc.toObject();
        expect(obj).toEqual({
          $id: "test",
          name: "Test",
          age: 25,
        });
      });

      test("should convert nested Doc instances", () => {
        const doc = new Doc({
          $id: "parent",
          child: { $id: "child", name: "Child" },
        });

        const obj = doc.toObject();
        expect(obj.child).not.toBeInstanceOf(Doc);
        expect(obj.child).toEqual({ $id: "child", name: "Child" });
      });

      test("should convert Doc instances in arrays", () => {
        const doc = new Doc({
          $id: "parent",
          children: [
            { $id: "child1", name: "Child 1" },
            { $id: "child2", name: "Child 2" },
          ],
        });

        const obj = doc.toObject();
        expect(obj.children[0]).not.toBeInstanceOf(Doc);
        expect(obj.children).toEqual([
          { $id: "child1", name: "Child 1" },
          { $id: "child2", name: "Child 2" },
        ]);
      });

      test("should filter by allow list", () => {
        const doc = new Doc({
          $id: "test",
          name: "Test",
          age: 25,
          email: "test@test.com",
        });

        const obj = doc.toObject(["$id", "name"]);
        expect(obj).toEqual({
          $id: "test",
          name: "Test",
        });
      });

      test("should filter by disallow list", () => {
        const doc = new Doc({
          $id: "test",
          name: "Test",
          age: 25,
          email: "test@test.com",
        });

        const obj = doc.toObject([], ["age", "email"]);
        expect(obj).toEqual({
          $id: "test",
          name: "Test",
        });
      });

      test("should apply both allow and disallow lists", () => {
        const doc = new Doc({
          $id: "test",
          name: "Test",
          age: 25,
          email: "test@test.com",
        });

        const obj = doc.toObject(["$id", "name", "age"], ["age"]);
        expect(obj).toEqual({
          $id: "test",
          name: "Test",
        });
      });
    });

    describe("toJSON()", () => {
      test("should return same as toObject()", () => {
        const doc = new Doc({
          $id: "test",
          name: "Test",
          child: { $id: "child" },
        });

        expect(doc.toJSON()).toEqual(doc.toObject());
      });
    });

    describe("clone()", () => {
      test("should create deep clone of document", () => {
        const doc = new Doc({
          $id: "test",
          name: "Test",
          age: 25,
        });

        const cloned = doc.clone();
        expect(cloned).not.toBe(doc);
        expect(cloned.toObject()).toEqual(doc.toObject());
      });

      test("should clone nested Doc instances", () => {
        const doc = new Doc({
          $id: "parent",
          child: { $id: "child", name: "Child" },
        });

        const cloned = doc.clone();
        const originalChild = doc.get("child");
        const clonedChild = cloned.get("child");

        expect(clonedChild).not.toBe(originalChild);
        expect(clonedChild).toBeInstanceOf(Doc);
        expect(clonedChild.get("$id")).toBe("child");
      });

      test("should clone arrays with Doc instances", () => {
        const doc = new Doc({
          $id: "parent",
          children: [{ $id: "child1" }, { $id: "child2" }],
        });

        const cloned = doc.clone();
        const originalChildren = doc.get("children");
        const clonedChildren = cloned.get("children");

        expect(clonedChildren).not.toBe(originalChildren);
        expect(clonedChildren[0]).not.toBe(originalChildren[0]);
        expect(clonedChildren[0]).toBeInstanceOf(Doc);
      });

      test("modifications to clone should not affect original", () => {
        const doc = new Doc({
          $id: "test",
          name: "Original",
        });

        const cloned = doc.clone();
        cloned.set("name", "Modified");

        expect(doc.get("name")).toBe("Original");
        expect(cloned.get("name")).toBe("Modified");
      });
    });
  });

  describe("Edge Cases", () => {
    test("should handle deeply nested structures", () => {
      const doc = new Doc({
        $id: "root",
        level1: {
          $id: "l1",
          level2: {
            $id: "l2",
            level3: {
              $id: "l3",
              value: "deep",
            },
          },
        },
      });

      const l1 = doc.get("level1");
      const l2 = l1.get("level2");
      const l3 = l2.get("level3");

      expect(l1).toBeInstanceOf(Doc);
      expect(l2).toBeInstanceOf(Doc);
      expect(l3).toBeInstanceOf(Doc);
      expect(l3.get("value")).toBe("deep");
    });

    test("should handle mixed arrays", () => {
      const doc = new Doc({
        $id: "test",
        mixed: ["string", 123, true, null, { $id: "doc" }, ["nested", "array"]],
      });

      const mixed = doc.get("mixed");
      expect(mixed[0]).toBe("string");
      expect(mixed[1]).toBe(123);
      expect(mixed[2]).toBe(true);
      expect(mixed[3]).toBe(null);
      expect(mixed[4]).toBeInstanceOf(Doc);
      expect(Array.isArray(mixed[5])).toBe(true);
    });

    test("should handle empty arrays", () => {
      const doc = new Doc({
        $id: "test",
        emptyArray: [],
      });

      expect(doc.get("emptyArray")).toEqual([]);
      expect(Array.isArray(doc.get("emptyArray"))).toBe(true);
    });

    test("should handle object with $collection but no $id", () => {
      const doc = new Doc({
        $id: "test",
        ref: { $collection: "users", name: "Test" },
      });

      const ref = doc.get("ref");
      expect(ref).toBeInstanceOf(Doc);
      expect(ref.get("$collection")).toBe("users");
      expect(ref.get("name")).toBe("Test");
    });

    test("should handle circular reference prevention", () => {
      const doc1 = new Doc({ $id: "doc1" });
      const doc2 = new Doc({ $id: "doc2" });

      // Set references to each other
      doc1.set("reference", doc2);
      doc2.set("reference", doc1);

      // Should not throw or cause infinite loop
      expect(doc1.get("reference")).toBe(doc2);
      expect(doc2.get("reference")).toBe(doc1);
    });
  });

  describe("Method Chaining", () => {
    test("should support chaining set operations", () => {
      const doc = new Doc()
        .set("$id", "test")
        .set("name", "Test")
        .set("age", 25);

      expect(doc.get("$id")).toBe("test");
      expect(doc.get("name")).toBe("Test");
      expect(doc.get("age")).toBe(25);
    });

    test("should support chaining update operations", () => {
      const doc = new Doc({ $id: "test", name: "Original" })
        .update("name", "Updated")
        .update("age" as unknown as any, 30);

      expect(doc.get("name")).toBe("Updated");
      expect(doc.get("age")).toBe(30);
    });

    test("should support chaining array operations", () => {
      const doc = new Doc({ $id: "test", items: [] as string[] })
        .append("items", "first")
        .append("items", "second")
        .prepend("items", "zero");

      expect(doc.get("items")).toEqual(["zero", "first", "second"]);
    });

    test("should support chaining delete operations", () => {
      const doc = new Doc({
        $id: "test",
        field1: "value1",
        field2: "value2",
        field3: "value3",
      })
        .delete("field1")
        .delete("field2");

      expect(doc.has("field1")).toBe(false);
      expect(doc.has("field2")).toBe(false);
      expect(doc.has("field3")).toBe(true);
    });
  });
});
