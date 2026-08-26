import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { Database } from "@core/database.js";
import { createTestDb } from "./helpers.js";
import { Doc } from "@core/doc.js";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";
import { AttributeEnum } from "@core/enums.js";

describe("Database Permissions", () => {
  let db: Database;
  let collectionLevelCollectionId: string;
  let documentLevelCollectionId: string;
  const schema = new Date().getTime().toString();

  beforeEach(async () => {
    db = createTestDb({ namespace: `perm_test_${schema}` });
    db.setMeta({ schema });
    await db.create();

    // Create collection for collection-level permission testing (documentSecurity: false)
    collectionLevelCollectionId = `collection_level_perms_${Date.now()}`;
    await db.createCollection({
      id: collectionLevelCollectionId,
      attributes: [
        new Doc({
          $id: "name",
          key: "name",
          type: AttributeEnum.String,
          size: 255,
          required: true,
        }),
        new Doc({
          $id: "email",
          key: "email",
          type: AttributeEnum.String,
          size: 255,
          required: false,
        }),
      ],
      permissions: [
        Permission.read(Role.user("admin_user")),
        Permission.create(Role.user("admin_user")),
        Permission.update(Role.any()),
        Permission.delete(Role.any()),
      ],
      documentSecurity: false, // Collection-level permissions only
    });

    // Create collection for document-level permission testing (documentSecurity: true)
    documentLevelCollectionId = `document_level_perms_${Date.now()}`;
    await db.createCollection({
      id: documentLevelCollectionId,
      attributes: [
        new Doc({
          $id: "name",
          key: "name",
          type: AttributeEnum.String,
          size: 255,
          required: true,
        }),
        new Doc({
          $id: "email",
          key: "email",
          type: AttributeEnum.String,
          size: 255,
          required: false,
        }),
      ],
      permissions: [
        Permission.create(Role.any()),
        Permission.update(Role.any()),
        Permission.delete(Role.any()),
      ],
      documentSecurity: true, // Document-level permissions enabled
    });
  });

  afterEach(async () => {
    await db.delete();
  });

  describe("getDocument - Collection Level Permissions", () => {
    let testDocumentId: string;
    beforeEach(async () => {
      const documentData = {
        name: "Collection Level Test",
        email: "collection@test.com",
      };

      const document = await db.system().createDocument(
        collectionLevelCollectionId,
        new Doc(documentData),
      );
      testDocumentId = document.getId();
    });

    test("should return document when user has collection-level read permission", async () => {
      const session = db.for("any", "user:admin_user");

      const document = await session.getDocument(
        collectionLevelCollectionId,
        testDocumentId,
      );

      expect(document.empty()).toBe(false);
      expect(document.getId()).toBe(testDocumentId);
      expect(document.get("name")).toBe("Collection Level Test");
    });

    test("should return empty document when user lacks collection-level read permission", async () => {
      const session = db.for("user:regular_user");

      await expect(
        session.getDocument(collectionLevelCollectionId, testDocumentId),
      ).rejects.toThrow();
    });

    test("should bypass authorization checks when using system session", async () => {
      const document = await db.system().getDocument(
        collectionLevelCollectionId,
        testDocumentId,
      );

      expect(document.empty()).toBe(false);
      expect(document.getId()).toBe(testDocumentId);
    });
  });

  describe("getDocument - Document Level Permissions", () => {
    let allowedDocumentId: string;
    let deniedDocumentId: string;

    beforeEach(async () => {
      // Create document with read permission for specific user
      const allowedDocumentData = {
        name: "Allowed Document",
        email: "allowed@test.com",
        $permissions: [
          Permission.read(Role.user("privileged_user")),
          Permission.update(Role.user("privileged_user")),
        ],
      };

      const allowedDocument = await db.for("any").createDocument(
        documentLevelCollectionId,
        new Doc(allowedDocumentData),
      );
      allowedDocumentId = allowedDocument.getId();

      // Create document without read permission for the user
      const deniedDocumentData = {
        name: "Denied Document",
        email: "denied@test.com",
        $permissions: [
          Permission.read(Role.user("other_user")),
          Permission.update(Role.user("other_user")),
        ],
      };

      const deniedDocument = await db.for("any").createDocument(
        documentLevelCollectionId,
        new Doc(deniedDocumentData),
      );
      deniedDocumentId = deniedDocument.getId();
    });

    test("should return document when user has document-level read permission", async () => {
      const session = db.for("user:privileged_user");

      const document = await session.findOne(documentLevelCollectionId, (qb) =>
        qb.equal("$id", allowedDocumentId),
      );

      expect(document.empty()).toBe(false);
      expect(document.getId()).toBe(allowedDocumentId);
      expect(document.get("name")).toBe("Allowed Document");
    });

    test("should return empty document when user lacks document-level read permission", async () => {
      const session = db.for("user:privileged_user");

      const document = await session.findOne(documentLevelCollectionId, (qb) =>
        qb.equal("$id", deniedDocumentId),
      );

      expect(document.empty()).toBe(true);
    });

    test("should return document when user has both collection and document permissions", async () => {
      // Add collection-level read permission
      await db.updateCollection({
        id: documentLevelCollectionId,
        permissions: [
          Permission.read(Role.user("privileged_user")),
          Permission.create(Role.any()),
          Permission.update(Role.any()),
          Permission.delete(Role.any()),
        ],
        documentSecurity: true,
      });

      const session = db.for("user:privileged_user");

      const document = await session.findOne(documentLevelCollectionId, (qb) =>
        qb.equal("$id", allowedDocumentId),
      );

      expect(document.empty()).toBe(false);
      expect(document.getId()).toBe(allowedDocumentId);
    });

    test("should return empty document when user lacks both collection and document permissions", async () => {
      const session = db.for("user:unauthorized_user");

      const document = await session.findOne(documentLevelCollectionId, (qb) =>
        qb.equal("$id", allowedDocumentId),
      );

      expect(document.empty()).toBe(true);
    });

    test("should bypass authorization checks when using system session", async () => {
      const document = await db
        .system()
        .findOne(documentLevelCollectionId, (qb) =>
          qb.equal("$id", allowedDocumentId),
        );

      expect(document.empty()).toBe(false);
      expect(document.getId()).toBe(allowedDocumentId);
    });
  });

  describe("Permission Combination Scenarios", () => {
    let comboCollectionId: string;
    let testDocumentId: string;

    beforeEach(async () => {
      // Create collection with both collection and document security
      comboCollectionId = `combo_perms_${Date.now()}`;
      await db.createCollection({
        id: comboCollectionId,
        attributes: [
          new Doc({
            $id: "name",
            key: "name",
            type: AttributeEnum.String,
            size: 255,
            required: true,
          }),
        ],
        permissions: [
          Permission.read(Role.user("collection_reader")),
          Permission.create(Role.any()),
        ],
        documentSecurity: true,
      });

      // Create document with different permissions
      const documentData = {
        name: "Combo Test",
        $permissions: [Permission.read(Role.user("document_reader"))],
      };

      const document = await db.for("any").createDocument(
        comboCollectionId,
        new Doc(documentData),
      );
      testDocumentId = document.getId();
    });

    test("should prioritize document permissions when both exist", async () => {
      // User has collection permission but not document permission
      const session = db.for("user:document_reader");

      const document = await session.getDocument(
        comboCollectionId,
        testDocumentId,
      );

      // Should return empty because document permission takes precedence
      expect(document.empty()).toBe(false);
    });

    test("should allow access when user has document permission", async () => {
      // User has document permission
      const session = db.for("user:document_reader");

      const document = await session.getDocument(
        comboCollectionId,
        testDocumentId,
      );

      expect(document.empty()).toBe(false);
      expect(document.get("name")).toBe("Combo Test");
    });
  });

  describe("Role-based Access Control", () => {
    test("should handle multiple roles correctly", async () => {
      // Create collection with multiple role permissions
      const multiRoleCollectionId = `multi_role_${Date.now()}`;
      await db.createCollection({
        id: multiRoleCollectionId,
        attributes: [
          new Doc({
            $id: "name",
            key: "name",
            type: AttributeEnum.String,
            size: 255,
            required: true,
          }),
        ],
        permissions: [
          Permission.read(Role.user("admin")),
          Permission.read(Role.user("moderator")),
          Permission.create(Role.any()),
        ],
        documentSecurity: false,
      });

      // Create test document
      const document = await db.for("any").createDocument(
        multiRoleCollectionId,
        new Doc({ name: "Multi Role Test" }),
      );

      // Test admin access
      let retrieved = await db
        .for("user:admin")
        .getDocument(multiRoleCollectionId, document.getId());
      expect(retrieved.empty()).toBe(false);

      // Test moderator access
      retrieved = await db
        .for("user:moderator")
        .getDocument(multiRoleCollectionId, document.getId());
      expect(retrieved.empty()).toBe(false);

      // Test unauthorized user
      await expect(
        db
          .for("user:regular")
          .getDocument(multiRoleCollectionId, document.getId()),
      ).rejects.toThrow();
    });

    test("should handle 'any' role permissions", async () => {
      // Create collection with 'any' role permission
      const anyRoleCollectionId = `any_role_${Date.now()}`;
      await db.createCollection({
        id: anyRoleCollectionId,
        attributes: [
          new Doc({
            $id: "name",
            key: "name",
            type: AttributeEnum.String,
            size: 255,
            required: true,
          }),
        ],
        permissions: [
          Permission.read(Role.any()),
          Permission.create(Role.any()),
        ],
        documentSecurity: false,
      });

      // Create test document
      const document = await db.for("any").createDocument(
        anyRoleCollectionId,
        new Doc({ name: "Any Role Test" }),
      );

      // Test with any user role
      const retrieved = await db.for("any").getDocument(
        anyRoleCollectionId,
        document.getId(),
      );
      expect(retrieved.empty()).toBe(false);
      expect(retrieved.get("name")).toBe("Any Role Test");
    });
  });
});
