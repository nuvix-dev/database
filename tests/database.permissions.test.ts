import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { Database } from "@core/database.js";
import { createTestDb } from "./helpers.js";
import { Doc } from "@core/doc.js";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";
import { Authorization } from "@utils/authorization.js";
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

    // Enable authorization for permission tests
    Authorization.enable();
    // Set role that can create documents
    Authorization.setRole("any");

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
    // Reset authorization state
    Authorization.disable();
    Authorization.cleanRoles();
    await db.delete();
  });

  describe("getDocument - Collection Level Permissions", () => {
    let testDocumentId: string;
    beforeEach(async () => {
      const documentData = {
        name: "Collection Level Test",
        email: "collection@test.com",
      };

      const document = await Authorization.skip(() =>
        db.createDocument(collectionLevelCollectionId, new Doc(documentData)),
      );
      testDocumentId = document.getId();
    });

    test("should return document when user has collection-level read permission", async () => {
      // Clean previous roles and set admin user
      Authorization.cleanRoles();
      Authorization.setRole("any");
      Authorization.setRole("user:admin_user");

      const document = await db.getDocument(
        collectionLevelCollectionId,
        testDocumentId,
      );

      expect(document.empty()).toBe(false);
      expect(document.getId()).toBe(testDocumentId);
      expect(document.get("name")).toBe("Collection Level Test");
    });

    test("should return empty document when user lacks collection-level read permission", async () => {
      // Clean previous roles and set unauthorized user
      Authorization.cleanRoles();
      Authorization.unsetRole("any");
      Authorization.setRole("user:regular_user");

      await expect(
        db.getDocument(collectionLevelCollectionId, testDocumentId),
      ).rejects.toThrow();
    });

    test("should return document when authorization is disabled", async () => {
      // Disable authorization
      Authorization.disable();

      const document = await db.getDocument(
        collectionLevelCollectionId,
        testDocumentId,
      );

      expect(document.empty()).toBe(false);
      expect(document.getId()).toBe(testDocumentId);

      // Re-enable for other tests
      Authorization.enable();
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

      const allowedDocument = await db.createDocument(
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

      const deniedDocument = await db.createDocument(
        documentLevelCollectionId,
        new Doc(deniedDocumentData),
      );
      deniedDocumentId = deniedDocument.getId();
    });

    test("should return document when user has document-level read permission", async () => {
      // Clean previous roles and set privileged user
      Authorization.cleanRoles();
      Authorization.setRole("user:privileged_user");

      const document = await db.findOne(documentLevelCollectionId, (qb) =>
        qb.equal("$id", allowedDocumentId),
      );

      expect(document.empty()).toBe(false);
      expect(document.getId()).toBe(allowedDocumentId);
      expect(document.get("name")).toBe("Allowed Document");
    });

    test("should return empty document when user lacks document-level read permission", async () => {
      // Clean previous roles and set privileged user
      Authorization.cleanRoles();
      Authorization.setRole("user:privileged_user");

      const document = await db.findOne(documentLevelCollectionId, (qb) =>
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

      // Clean previous roles and set privileged user
      Authorization.cleanRoles();
      Authorization.setRole("user:privileged_user");

      const document = await db.findOne(documentLevelCollectionId, (qb) =>
        qb.equal("$id", allowedDocumentId),
      );

      expect(document.empty()).toBe(false);
      expect(document.getId()).toBe(allowedDocumentId);
    });

    test("should return empty document when user lacks both collection and document permissions", async () => {
      // Clean previous roles and set unauthorized user
      Authorization.cleanRoles();
      Authorization.setRole("user:unauthorized_user");

      const document = await db.findOne(documentLevelCollectionId, (qb) =>
        qb.equal("$id", allowedDocumentId),
      );

      expect(document.empty()).toBe(true);
    });

    test("should return document when authorization is disabled", async () => {
      // Disable authorization
      Authorization.disable();

      const document = await db.findOne(documentLevelCollectionId, (qb) =>
        qb.equal("$id", allowedDocumentId),
      );

      expect(document.empty()).toBe(false);
      expect(document.getId()).toBe(allowedDocumentId);

      // Re-enable for other tests
      Authorization.enable();
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

      const document = await db.createDocument(
        comboCollectionId,
        new Doc(documentData),
      );
      testDocumentId = document.getId();
    });

    test("should prioritize document permissions when both exist", async () => {
      // User has collection permission but not document permission
      Authorization.cleanRoles();
      Authorization.setRole("user:document_reader");

      const document = await db.getDocument(comboCollectionId, testDocumentId);

      // Should return empty because document permission takes precedence
      expect(document.empty()).toBe(false);
    });

    test("should allow access when user has document permission", async () => {
      // User has document permission
      Authorization.cleanRoles();
      Authorization.setRole("user:document_reader");

      const document = await db.getDocument(comboCollectionId, testDocumentId);

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
      const document = await db.createDocument(
        multiRoleCollectionId,
        new Doc({ name: "Multi Role Test" }),
      );

      // Test admin access
      Authorization.cleanRoles();
      Authorization.setRole("user:admin");
      let retrieved = await db.getDocument(
        multiRoleCollectionId,
        document.getId(),
      );
      expect(retrieved.empty()).toBe(false);

      // Clean roles and test moderator access
      Authorization.cleanRoles();
      Authorization.setRole("user:moderator");
      retrieved = await db.getDocument(multiRoleCollectionId, document.getId());
      expect(retrieved.empty()).toBe(false);

      // Clean roles and test unauthorized user
      Authorization.cleanRoles();
      Authorization.setRole("user:regular");
      await expect(
        db.getDocument(multiRoleCollectionId, document.getId()),
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
      const document = await db.createDocument(
        anyRoleCollectionId,
        new Doc({ name: "Any Role Test" }),
      );

      // Test with any user role
      Authorization.cleanRoles();
      Authorization.setRole("any");
      const retrieved = await db.getDocument(
        anyRoleCollectionId,
        document.getId(),
      );
      expect(retrieved.empty()).toBe(false);
      expect(retrieved.get("name")).toBe("Any Role Test");
    });
  });
});
