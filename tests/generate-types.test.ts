import { describe, it, expect } from "vitest";
import {
  generateTypes,
  generateDocType,
  generateEntityType,
} from "../src/utils/generate-types.js";
import { AttributeEnum } from "@core/enums.js";
import { Database } from "index.js";
import { writeFile } from "fs/promises";

describe("generateTypes", () => {
  it("generates IEntity base interface correctly", () => {
    const result = generateTypes([]);

    expect(result).toContain("$id: string");
    expect(result).toContain("$createdAt: Date | string | null");
    expect(result).toContain("$updatedAt: Date | string | null");
    expect(result).toContain("$permissions: string[]");
    expect(result).toContain("$sequence: number");
    expect(result).toContain("$collection: string");
    expect(result).toContain("$tenant?: number | null");
  });

  it("generates empty Entities interface for empty collections array", () => {
    const result = generateTypes([]);

    expect(result).toContain("export interface Entities {");
    expect(result).toContain("}");
  });

  it("maps all basic attribute types correctly", () => {
    const collection = {
      $id: "test",
      name: "test_collection",
      $collection: Database.METADATA,
      attributes: [
        {
          $id: "str_field",
          key: "str_field",
          type: AttributeEnum.String,
          required: true,
          array: false,
        },
        {
          $id: "int_field",
          key: "int_field",
          type: AttributeEnum.Integer,
          required: true,
          array: false,
        },
        {
          $id: "float_field",
          key: "float_field",
          type: AttributeEnum.Float,
          required: true,
          array: false,
        },
        {
          $id: "bool_field",
          key: "bool_field",
          type: AttributeEnum.Boolean,
          required: true,
          array: false,
        },
        {
          $id: "timestamp_field",
          key: "timestamp_field",
          type: AttributeEnum.Timestamptz,
          required: true,
          array: false,
        },
        {
          $id: "json_field",
          key: "json_field",
          type: AttributeEnum.Json,
          required: true,
          array: false,
        },
        {
          $id: "uuid_field",
          key: "uuid_field",
          type: AttributeEnum.Uuid,
          required: true,
          array: false,
        },
        {
          $id: "virtual_field",
          key: "virtual_field",
          type: AttributeEnum.Virtual,
          required: true,
          array: false,
        },
      ],
      indexes: [],
      enabled: true,
      documentSecurity: false,
    };

    const result = generateTypes([collection]);

    expect(result).toContain("str_field: string");
    expect(result).toContain("int_field: number");
    expect(result).toContain("float_field: number");
    expect(result).toContain("bool_field: boolean");
    expect(result).toContain("timestamp_field: string | Date");
    expect(result).toContain("json_field: Record<string, any>");
    expect(result).toContain("uuid_field: string");
    expect(result).toContain("virtual_field: never");
  });

  it("handles array types correctly", () => {
    const collection = {
      $id: "test",
      name: "test_collection",
      $collection: "test_collection",
      attributes: [
        {
          $id: "string_array",
          key: "string_array",
          type: AttributeEnum.String,
          required: true,
          array: true,
        },
        {
          $id: "number_array",
          key: "number_array",
          type: AttributeEnum.Integer,
          required: true,
          array: true,
        },
      ],
      indexes: [],
      enabled: true,
      documentSecurity: false,
    };

    const result = generateTypes([collection]);

    expect(result).toContain("string_array: string[]");
    expect(result).toContain("number_array: number[]");
  });

  it("handles optional fields correctly", async () => {
    const collection = {
      $id: "test",
      name: "test_collection",
      $collection: "test_collection",
      attributes: [
        {
          $id: "required_field",
          key: "required_field",
          type: AttributeEnum.String,
          required: true,
          array: false,
        },
        {
          $id: "optional_field",
          key: "optional_field",
          type: AttributeEnum.String,
          required: false,
          array: false,
        },
      ],
      indexes: [],
      enabled: true,
      documentSecurity: false,
    };

    const result = generateTypes([collection]);
    await writeFile("tests/generated-types.ts", result);

    expect(result).toContain("required_field: string");
    expect(result).toContain("optional_field?: string");
  });

  it("handles enum literal types correctly", () => {
    const collection = {
      $id: "test",
      name: "test_collection",
      $collection: "test_collection",
      attributes: [
        {
          $id: "status_field",
          key: "status_field",
          type: AttributeEnum.String,
          format: "enum",
          formatOptions: {
            values: ["active", "inactive", "pending"],
          },
          required: true,
          array: false,
        },
      ],
      indexes: [],
      enabled: true,
      documentSecurity: false,
    };

    const result = generateTypes([collection]);

    expect(result).toContain('status_field: "active" | "inactive" | "pending"');
  });

  it("handles relationship types with valid related collection", () => {
    const userCollection = {
      $id: "users",
      name: "users",
      $collection: "users",
      attributes: [
        {
          $id: "name",
          key: "name",
          type: AttributeEnum.String,
          required: true,
          array: false,
        },
      ],
      indexes: [],
      enabled: true,
      documentSecurity: false,
    };

    const postCollection = {
      $id: "posts",
      name: "posts",
      $collection: "posts",
      attributes: [
        {
          $id: "author_id",
          key: "author_id",
          type: AttributeEnum.Relationship,
          options: {
            relatedCollection: "users",
          },
          required: true,
          array: false,
        },
      ],
      indexes: [],
      enabled: true,
      documentSecurity: false,
    };

    const result = generateTypes([userCollection, postCollection]);

    expect(result).toContain("author_id: Users['$id']");
  });

  it("handles relationship types with invalid related collection", () => {
    const collection = {
      $id: "posts",
      name: "posts",
      $collection: "posts",
      attributes: [
        {
          $id: "author_id",
          key: "author_id",
          type: AttributeEnum.Relationship,
          options: {
            relatedCollection: "nonexistent",
          },
          required: true,
          array: false,
        },
      ],
      indexes: [],
      enabled: true,
      documentSecurity: false,
    };

    const result = generateTypes([collection]);

    expect(result).toContain("author_id: string");
  });

  it("handles unknown attribute types with fallback", () => {
    const collection = {
      $id: "test",
      name: "test_collection",
      $collection: "test_collection",
      attributes: [
        {
          $id: "unknown_field",
          key: "unknown_field",
          type: "unknown_type" as AttributeEnum,
          required: true,
          array: false,
        },
      ],
      indexes: [],
      enabled: true,
      documentSecurity: false,
    };

    const result = generateTypes([collection]);

    expect(result).toContain("unknown_field: any");
  });

  it("generates correct interface names using pascalCase", () => {
    const collections = [
      {
        $id: "users",
        name: "users",
        $collection: "users",
        attributes: [],
        indexes: [],
        enabled: true,
        documentSecurity: false,
      },
      {
        $id: "user_posts",
        name: "user_posts",
        $collection: "user_posts",
        attributes: [],
        indexes: [],
        enabled: true,
        documentSecurity: false,
      },
      {
        $id: "api-keys",
        name: "api-keys",
        $collection: "api-keys",
        attributes: [],
        indexes: [],
        enabled: true,
        documentSecurity: false,
      },
      {
        $id: "special$chars",
        name: "special$chars",
        $collection: "special$chars",
        attributes: [],
        indexes: [],
        enabled: true,
        documentSecurity: false,
      },
    ];

    const result = generateTypes(collections);

    expect(result).toContain("export interface Users extends IEntity");
    expect(result).toContain("export interface UserPosts extends IEntity");
    expect(result).toContain("export interface ApiKeys extends IEntity");
    expect(result).toContain("export interface SpecialChars extends IEntity");
  });

  it("generates correct Entities interface mapping", () => {
    const collections = [
      {
        $id: "users",
        name: "users",
        $collection: "users",
        attributes: [],
        indexes: [],
        enabled: true,
        documentSecurity: false,
      },
      {
        $id: "posts",
        name: "posts",
        $collection: "posts",
        attributes: [],
        indexes: [],
        enabled: true,
        documentSecurity: false,
      },
    ];

    const result = generateTypes(collections);

    expect(result).toContain("export interface Entities {");
    expect(result).toContain('"users": Users;');
    expect(result).toContain('"posts": Posts;');
  });

  it("handles collections with no attributes", () => {
    const collection = {
      $id: "empty",
      name: "empty_collection",
      $collection: "empty_collection",
      attributes: [],
      indexes: [],
      enabled: true,
      documentSecurity: false,
    };

    const result = generateTypes([collection]);

    expect(result).toContain(
      "export interface EmptyCollection extends IEntity {",
    );
    expect(result).toContain('"empty": EmptyCollection;');
  });

  it("handles complex mixed attribute scenarios", () => {
    const userCollection = {
      $id: "users",
      name: "users",
      $collection: "users",
      attributes: [
        {
          $id: "name",
          key: "name",
          type: AttributeEnum.String,
          required: true,
          array: false,
        },
      ],
      indexes: [],
      enabled: true,
      documentSecurity: false,
    };

    const postCollection = {
      $id: "posts",
      name: "posts",
      $collection: "posts",
      attributes: [
        {
          $id: "title",
          key: "title",
          type: AttributeEnum.String,
          required: true,
          array: false,
        },
        {
          $id: "tags",
          key: "tags",
          type: AttributeEnum.String,
          required: false,
          array: true,
        },
        {
          $id: "status",
          key: "status",
          type: AttributeEnum.String,
          format: "enum",
          formatOptions: {
            values: ["draft", "published"],
          },
          required: true,
          array: false,
        },
        {
          $id: "author_id",
          key: "author_id",
          type: AttributeEnum.Relationship,
          options: {
            relatedCollection: "users",
          },
          required: true,
          array: false,
        },
        {
          $id: "metadata",
          key: "metadata",
          type: AttributeEnum.Json,
          required: false,
          array: false,
        },
      ],
      indexes: [],
      enabled: true,
      documentSecurity: false,
    };

    const result = generateTypes([userCollection, postCollection]);

    expect(result).toContain("title: string");
    expect(result).toContain("tags?: string[]");
    expect(result).toContain('status: "draft" | "published"');
    expect(result).toContain("author_id: Users['$id']");
    expect(result).toContain("metadata?: Record<string, any>");
  });

  it("handles edge cases in pascalCase conversion", () => {
    const collections = [
      {
        $id: "test1",
        name: "",
        $collection: "",
        attributes: [],
        indexes: [],
        enabled: true,
        documentSecurity: false,
      },
      {
        $id: "test2",
        name: "___",
        $collection: "___",
        attributes: [],
        indexes: [],
        enabled: true,
        documentSecurity: false,
      },
      {
        $id: "test3",
        name: "123numbers",
        $collection: "123numbers",
        attributes: [],
        indexes: [],
        enabled: true,
        documentSecurity: false,
      },
      {
        $id: "test4",
        name: "multiple---dashes",
        $collection: "multiple---dashes",
        attributes: [],
        indexes: [],
        enabled: true,
        documentSecurity: false,
      },
    ];

    const result = generateTypes(collections);

    expect(result).toContain("export interface  extends IEntity"); // empty name
    expect(result).toContain("export interface  extends IEntity"); // underscores only
    expect(result).toContain("export interface 123numbers extends IEntity");
    expect(result).toContain("export interface MultipleDashes extends IEntity");
  });

  describe("Enhanced Features", () => {
    it("generates Doc types for collections", () => {
      const collections = [
        {
          $id: "users",
          name: "users",
          $collection: "users",
          attributes: [
            {
              $id: "name",
              key: "name",
              type: AttributeEnum.String,
              required: true,
              array: false,
            },
          ],
          indexes: [],
          enabled: true,
          documentSecurity: false,
        },
        {
          $id: "posts",
          name: "posts",
          $collection: "posts",
          attributes: [
            {
              $id: "title",
              key: "title",
              type: AttributeEnum.String,
              required: true,
              array: false,
            },
          ],
          indexes: [],
          enabled: true,
          documentSecurity: false,
        },
      ];

      const result = generateTypes(collections);

      expect(result).toContain('import { Doc, IEntity } from "@nuvix/db";');
      expect(result).toContain("export type UsersDoc = Doc<Users>;");
      expect(result).toContain("export type PostsDoc = Doc<Posts>;");
    });

    it("generates individual Doc type", () => {
      const collection = {
        $id: "users",
        name: "users",
        $collection: "users",
        attributes: [],
        indexes: [],
        enabled: true,
        documentSecurity: false,
      };

      const result = generateDocType(collection);
      expect(result).toBe("export type UsersDoc = Doc<Users>;");
    });

    it("generates individual entity type", () => {
      const collection = {
        $id: "users",
        name: "users",
        $collection: "users",
        attributes: [
          {
            $id: "name",
            key: "name",
            type: AttributeEnum.String,
            required: true,
            array: false,
          },
        ],
        indexes: [],
        enabled: true,
        documentSecurity: false,
      };

      const result = generateEntityType(collection, [collection]);
      expect(result).toContain("export interface Users extends IEntity");
      expect(result).toContain("name: string");
    });

    it("supports custom package name", () => {
      const collections = [
        {
          $id: "users",
          name: "users",
          $collection: "users",
          attributes: [],
          indexes: [],
          enabled: true,
          documentSecurity: false,
        },
      ];

      const result = generateTypes(collections, {
        packageName: "@custom/db-package",
      });

      expect(result).toContain(
        'import { Doc, IEntity } from "@custom/db-package";',
      );
    });

    it("supports selective generation options", () => {
      const collections = [
        {
          $id: "users",
          name: "users",
          $collection: "users",
          attributes: [],
          indexes: [],
          enabled: true,
          documentSecurity: false,
        },
      ];

      const result = generateTypes(collections, {
        includeImports: false,
        includeDocTypes: false,
        includeEntityMap: false,
      });

      expect(result).not.toContain("import { Doc }");
      expect(result).not.toContain("export interface IEntity");
      expect(result).not.toContain("export type UsersDoc");
      expect(result).not.toContain("export interface Entities");
      expect(result).toContain("export interface Users extends IEntity");
    });

    it("generates enhanced attribute comments", () => {
      const collection = {
        $id: "test",
        name: "test_collection",
        $collection: "test_collection",
        attributes: [
          {
            $id: "email",
            key: "email",
            type: AttributeEnum.String,
            format: "email",
            formatOptions: {
              pattern: "^[^@]+@[^@]+\\.[^@]+$",
              minLength: 5,
              maxLength: 100,
            },
            required: true,
            array: false,
            default: "user@example.com",
          },
          {
            $id: "age",
            key: "age",
            type: AttributeEnum.Integer,
            formatOptions: {
              min: 0,
              max: 120,
            },
            required: false,
            array: false,
          },
          {
            $id: "tags",
            key: "tags",
            type: AttributeEnum.String,
            required: false,
            array: true,
          },
        ],
        indexes: [],
        enabled: true,
        documentSecurity: false,
      };

      const result = generateTypes([collection]);

      expect(result).toContain("@format email");
      expect(result).toContain("@pattern ^[^@]+@[^@]+\\.[^@]+$");
      expect(result).toContain("@minLength 5");
      expect(result).toContain("@maxLength 100");
      expect(result).toContain('@default "user@example.com"');
      expect(result).toContain("@min 0");
      expect(result).toContain("@max 120");
      expect(result).toContain("@optional");
      expect(result).toContain("@array");
    });
  });
});
