/**
 * Admin/schema plane of the {@link Database} facade.
 *
 * Extracted from the Database god class (Phase 2). Owns every
 * collection/attribute/relationship/index operation plus database
 * create/exists/list/delete. Database remains a thin facade delegating to
 * this collaborator; public method signatures are unchanged.
 *
 * Database is imported type-only to avoid runtime circular imports — the
 * same pattern used by index-manager.ts and relationship-schema.ts.
 *
 * Members that standalone modules cannot reach through the type system
 * (the protected validation flag, attribute/default validators, enabled
 * guard and metadata collection constant on Base) are injected by the
 * Database constructor as bound closures/values. The closures read state
 * lazily, so every transactional scope observes its own configuration.
 */
import type { Database } from "./database.js";
import { Base } from "./base.js";
import { AttributeEnum, EventsEnum, OnDelete } from "./enums.js";
import { Attribute, Collection, Index } from "@validators/schema.js";
import {
  CreateCollection,
  CreateRelationshipAttribute,
  UpdateCollection,
  UpdateRelationshipAttribute,
} from "./types.js";
import type { Entities } from "@nuvix/db";
import { Query } from "./query.js";
import { Doc } from "./doc.js";
import {
  DatabaseException,
  DependencyException,
  DuplicateException,
  IndexException,
  LimitException,
  NotFoundException,
} from "@errors/index.js";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";
import { Permissions } from "@validators/permissions.js";
import { Index as IndexValidator } from "@validators/index-validator.js";
import { Structure } from "@validators/structure.js";
import { IndexDependency } from "@validators/index-dependency.js";
import {
  createIndex,
  deleteIndex,
  renameIndex,
  updateIndexMeta,
} from "./index-manager.js";
import {
  createRelationship,
  deleteRelationship,
  updateRelationship,
} from "./relationship-schema.js";

/**
 * Privileged operations the schema plane needs from its owning Database.
 * Injected rather than reached through the type system because the
 * underlying Base members are protected; see class docs for why the
 * members are lazy.
 */
export interface SchemaInternals {
  /** Current validation flag (mutable per scope via enable/disableValidation). */
  readonly validate: () => boolean;
  /** The metadata collection definition (`Base.COLLECTION`, read-only usage). */
  readonly metadataCollection: Collection;
  validateDefaultTypes(type: AttributeEnum, value: unknown): void;
  validateAttribute(
    collection: Doc<Collection>,
    attribute: Attribute,
  ): Promise<Doc<Attribute>>;
  assertCollectionEnabled(collection: Doc<Collection>): boolean;
}

export class SchemaManager {
  /** The owning facade's adapter, resolved per call (scopes swap adapters). */
  private get adapter() {
    return this.db.getAdapter();
  }

  constructor(
    private readonly db: Database,
    private readonly internals: SchemaInternals,
  ) {}

  /**
   * Creates a new database.
   */
  public async create(database?: string): Promise<void> {
    database = database ?? this.adapter.$schema;
    await this.adapter.create(database);

    const attributes = [...this.internals.metadataCollection.attributes].map(
      (attr) => new Doc(attr),
    );
    await this.db.silent(() =>
      this.createCollection({ id: Base.METADATA, attributes }),
    );

    this.db.trigger(EventsEnum.DatabaseCreate, database);
  }

  /**
   * Check is database or collection already exists or not.
   */
  public async exists<C extends keyof Entities>(
    database?: string,
    collection?: C,
  ): Promise<boolean>;
  public async exists(database?: string, collection?: string): Promise<boolean>;
  public async exists(
    database?: string,
    collection?: string,
  ): Promise<boolean> {
    database ??= this.adapter.$schema;
    return this.adapter.exists(database, collection);
  }

  /**
   * list of databases.
   */
  public async list(): Promise<string[]> {
    this.db.trigger(EventsEnum.DatabaseList, []);
    return [];
  }

  /**
   * Delete a database.
   */
  public async delete(database?: string): Promise<void> {
    database ??= this.adapter.$schema;
    await this.adapter.delete(database);

    this.db.trigger(EventsEnum.DatabaseDelete, database);
    await this.db.getCache().flush();
  }

  /**
   * Creates a new collection in the database.
   */
  public async createCollection({
    id,
    attributes = [],
    indexes = [],
    permissions,
    documentSecurity,
    enabled,
  }: CreateCollection): Promise<Doc<Collection>> {
    permissions ??= [
      Permission.create(Role.any()),
      Permission.read(Role.any()),
      Permission.update(Role.any()),
      Permission.delete(Role.any()),
    ];

    if (this.internals.validate()) {
      const perms = new Permissions();
      if (!perms.$valid(permissions)) {
        throw new DatabaseException(perms.$description);
      }
    }

    let collection = await this.db.silent(() => this.getCollection(id));
    if (!collection.empty() && id !== Base.METADATA) {
      throw new DuplicateException(`Collection '${id}' already exists.`);
    }

    // Fix metadata index orders
    for (let i = 0; i < indexes.length; i++) {
      const index = indexes[i]!;
      const orders: (string | null)[] = index.get("orders", []);

      const indexAttributes = index.get("attributes", []);
      for (let j = 0; j < indexAttributes.length; j++) {
        const attr = indexAttributes[j];
        for (const collectionAttribute of attributes) {
          if (collectionAttribute.get("$id") === attr) {
            const isArray = collectionAttribute.get("array", false);
            if (isArray) {
              orders[j] = null;
            }
            break;
          }
        }
      }

      index.set("orders", orders);
      indexes[i] = index;
    }

    collection = new Doc<Collection>({
      $id: id,
      $permissions: permissions,
      name: id,
      attributes: attributes,
      indexes: indexes,
      documentSecurity: documentSecurity ?? true,
      enabled: enabled ?? true,
    });

    if (this.internals.validate()) {
      const validator = new IndexValidator(
        attributes,
        this.adapter.$maxIndexLength,
        this.adapter.$internalIndexesKeys,
        this.adapter.$supportForIndexArray,
      );
      indexes.forEach((index) => {
        if (!validator.$valid(index)) {
          throw new IndexException(validator.$description);
        }
      });
    }

    if (
      indexes.length &&
      this.adapter.getCountOfIndexes(collection) > this.adapter.$limitForIndexes
    ) {
      throw new LimitException(
        `Index limit of ${this.adapter.$limitForIndexes} exceeded. Cannot create collection.`,
      );
    }

    if (attributes.length) {
      if (
        this.adapter.$limitForAttributes &&
        attributes.length > this.adapter.$limitForAttributes
      ) {
        throw new LimitException(
          `Attribute limit of ${this.adapter.$limitForAttributes} exceeded. Cannot create collection.`,
        );
      }
      if (
        this.adapter.$documentSizeLimit &&
        this.adapter.getAttributeWidth(collection) >
          this.adapter.$documentSizeLimit
      ) {
        throw new LimitException(
          `Document size limit of ${this.adapter.$documentSizeLimit} exceeded. Cannot create collection.`,
        );
      }
    }

    try {
      await this.adapter.createCollection({ name: id, attributes, indexes });
    } catch (error) {
      if (error instanceof DuplicateException) {
        // $HACK: Metadata should still be updated, can be removed when null tenant collections are supported.
        if (!this.db.sharedTables || !this.db.migrating) {
          throw error;
        }
      } else {
        throw error;
      }
    }

    if (id === Base.METADATA) {
      return new Doc(this.internals.metadataCollection);
    }

    const createdCollection = await this.db.silent(() =>
      this.db.system().createDocument(Base.METADATA, collection),
    );
    this.db.trigger(EventsEnum.CollectionCreate, createdCollection);

    return createdCollection;
  }

  /**
   * Update collection permissions & documentSecurity.
   */
  public async updateCollection({
    id,
    documentSecurity,
    permissions,
    enabled,
  }: UpdateCollection): Promise<Doc<Collection>> {
    if (permissions.length) {
      if (this.internals.validate()) {
        const perms = new Permissions();
        if (!perms.$valid(permissions)) {
          throw new DatabaseException(perms.$description);
        }
      }
    }

    let collection = await this.db.silent(() => this.getCollection(id, true));

    if (
      this.adapter.$sharedTables &&
      collection.getTenant() !== this.adapter.$tenantId
    ) {
      throw new NotFoundException(`Collection '${id}' not found`);
    }

    collection.set("$permissions", permissions);
    collection.set("documentSecurity", documentSecurity);
    collection.set("enabled", enabled);

    collection = await this.db.silent(() =>
      this.db.system().updateDocument<Doc<Collection>>(
        Base.METADATA,
        collection.getId(),
        collection,
      ),
    );
    this.db.trigger(EventsEnum.CollectionUpdate, collection);

    return collection;
  }

  /**
   * Retrieves a collection by its ID.
   * If the collection is not found or does not match the tenant ID, an empty Doc
   */
  public async getCollection(
    id: string,
    throwOnNotFound?: boolean,
  ): Promise<Doc<Collection>> {
    let collection = (await this.db.silent(() =>
      this.db.system().getDocument(Base.METADATA, id),
    )) as Doc<Collection>;

    if (
      id !== Base.METADATA &&
      this.adapter.$sharedTables &&
      collection.getTenant() !== null &&
      collection.getTenant() !== this.adapter.$tenantId
    ) {
      if (throwOnNotFound) {
        throw new NotFoundException(`Collection '${id}' not found`);
      }
      return new Doc<Collection>();
    }

    if (this.internals.assertCollectionEnabled(collection)) {
      collection = new Doc<Collection>();
    }

    this.db.trigger(EventsEnum.CollectionRead, collection);
    if (collection.empty() && throwOnNotFound) {
      throw new NotFoundException(`Collection '${id}' not found`);
    }

    return collection;
  }

  /**
   * Lists all collections in the database.
   */
  public async listCollections(
    limit: number = 25,
    offset: number = 0,
  ): Promise<Doc<Collection>[]> {
    const query = [Query.limit(limit), Query.offset(offset)];

    const metadataCollection: string = Base.METADATA;
    return (await this.db.system().find(metadataCollection, query)) as Doc<
      Collection
    >[];
  }

  /**
   * Gets the size of a collection.
   */
  public async getSizeOfCollection(collectionId: string): Promise<number> {
    const collection = await this.db.silent(() =>
      this.getCollection(collectionId, true),
    );

    if (
      this.adapter.$sharedTables &&
      collection.getTenant() !== this.adapter.$tenantId
    ) {
      throw new NotFoundException(`Collection '${collectionId}' not found`);
    }

    return this.adapter.getSizeOfCollection(collection.getId());
  }

  /**
   * Gets the size of a collection on Disk.
   */
  public async getSizeOfCollectionOnDisk(
    collectionId: string,
  ): Promise<number> {
    if (this.adapter.$sharedTables && !this.adapter.$tenantId) {
      throw new DatabaseException(
        "Missing tenant. Tenant must be set when table sharing is enabled.",
      );
    }

    const collection = await this.db.silent(() =>
      this.getCollection(collectionId, true),
    );

    if (
      this.adapter.$sharedTables &&
      collection.getTenant() !== this.adapter.$tenantId
    ) {
      throw new NotFoundException(`Collection '${collectionId}' not found`);
    }

    return this.adapter.getSizeOfCollectionOnDisk(collection.getId());
  }

  /**
   * Analyze collection.
   */
  public async analyzeCollection(collection: string): Promise<boolean> {
    return this.adapter.analyzeCollection(collection);
  }

  /**
   * Delete a collection by ID.
   */
  public async deleteCollection(id: string): Promise<boolean> {
    const collection = (await this.db.silent(() =>
      this.db.system().getDocument(Base.METADATA, id),
    )) as Doc<Collection>;

    if (collection.empty() || collection.getId() === Base.METADATA) {
      throw new NotFoundException(`Collection '${id}' not found`);
    }

    if (
      this.adapter.$sharedTables &&
      collection.getTenant() !== this.adapter.$tenantId
    ) {
      throw new NotFoundException(`Collection '${id}' not found`);
    }

    const relationships = collection
      .get("attributes", [])
      .filter(
        (attribute: Doc<Attribute>) =>
          attribute.get("type") === AttributeEnum.Relationship,
      );

    return await this.db.withTransaction(async (db) => {
      for (const relationship of relationships) {
        await db.deleteRelationship(
          collection.getId(),
          relationship.get("$id"),
        );
      }

      try {
        await db.getAdapter().deleteCollection(id);
      } catch (error) {
        if (error instanceof NotFoundException) {
          // HACK: Metadata should still be updated, can be removed when null tenant collections are supported.
          if (!db.getAdapter().$sharedTables || !this.db.migrating) {
            throw error;
          }
        } else {
          throw error;
        }
      }

      let deleted: boolean;
      if (id === Base.METADATA) {
        deleted = true;
      } else {
        deleted = await db.silent(() =>
          db.system().deleteDocument(Base.METADATA, id),
        );
      }

      if (deleted) {
        // todo:
        this.db.trigger(EventsEnum.CollectionDelete, collection);
      }

      await this.db.purgeCachedCollection(id);

      return deleted;
    });
  }

  /**
   * Creates an attribute in a collection.
   */
  public async createAttribute(collectionId: string, attribute: Attribute) {
    const type = attribute.type;
    if (type === AttributeEnum.Relationship || type === AttributeEnum.Virtual) {
      throw new DatabaseException(`Cannot create attribute of type '${type}'.`);
    }

    let collection = await this.db.silent(() =>
      this.getCollection(collectionId, true),
    );
    const attr = await this.internals.validateAttribute(collection, attribute);

    collection.append("attributes", attr);

    try {
      await this.adapter.createAttribute({
        collection: collectionId,
        ...attribute,
      });
    } catch (error) {
      if (error instanceof DuplicateException) {
        // HACK: Metadata should still be updated, can be removed when null tenant collections are supported.
        if (!this.adapter.$sharedTables || !this.db.migrating) {
          throw error;
        }
      } else throw error;
    }

    if (collection.getId() !== Base.METADATA) {
      collection = await this.db.silent(() =>
        this.db
          .system()
          .updateDocument<Doc<Collection>>(
            Base.METADATA,
            collection.getId(),
            collection,
          ),
      );
    }

    this.db.trigger(EventsEnum.AttributeCreate, collection, attr);
    return true;
  }

  /**
   * Creates multiple attributes in a collection.
   */
  public async createAttributes(
    collectionId: string,
    attributes: Attribute[],
  ) {
    if (attributes.length === 0) {
      throw new DatabaseException("No attributes to create");
    }

    let collection = await this.db.silent(() =>
      this.getCollection(collectionId, true),
    );
    const attrDocs: Doc<Attribute>[] = [];

    for (const attribute of attributes) {
      const attr = await this.internals.validateAttribute(collection, attribute);

      collection.append("attributes", attr);
      attrDocs.push(attr);
    }

    try {
      await this.adapter.createAttributes(collection.getId(), attributes);
    } catch (error) {
      if (error instanceof DuplicateException) {
        // No attributes were in a metadata, but at least one of them was present on the table
        // HACK: Metadata should still be updated, can be removed when null tenant collections are supported.
        if (!this.adapter.$sharedTables || !this.db.migrating) {
          throw error;
        }
      }
      throw error;
    }

    if (collection.getId() !== Base.METADATA) {
      collection = await this.db.silent(() =>
        this.db
          .system()
          .updateDocument<Doc<Collection>>(
            Base.METADATA,
            collection.getId(),
            collection,
          ),
      );
    }

    this.db.purgeCachedCollection(collection);
    this.db.purgeCachedDocument(Base.METADATA, collection);

    this.db.trigger(EventsEnum.AttributesCreate, collection, attrDocs);
    return true;
  }

  /**
   * Update index metadata. Utility method for update index methods.
   */
  public async updateIndexMeta(
    collectionId: string,
    id: string,
    updateCallback: (
      index: Doc<Index>,
      collection: Doc<Collection>,
      indexPosition: number,
    ) => void,
  ): Promise<Doc<Index>> {
    return updateIndexMeta(this.db, collectionId, id, updateCallback);
  }

  /**
   * Update attribute metadata. Utility method for update attribute methods.
   */
  public async updateAttributeMeta(
    collectionId: string,
    id: string,
    updateCallback: (
      attribute: Doc<Attribute>,
      collection: Doc<Collection>,
      index: number,
    ) => void | Promise<void>,
  ): Promise<Doc<Attribute>> {
    let collection = await this.db.silent(() =>
      this.getCollection(collectionId),
    );

    if (collection.getId() === Base.METADATA) {
      throw new DatabaseException("Cannot update metadata attributes");
    }

    const attributes = collection.get("attributes", []);
    const index = attributes.findIndex(
      (attribute: Doc<Attribute>) => attribute.get("$id") === id,
    );

    if (index === -1) {
      throw new NotFoundException("Attribute not found");
    }

    // Execute update from callback
    const res = updateCallback(attributes[index]!, collection, index);
    if (res instanceof Promise) {
      await res;
    }

    // Save
    collection.set("attributes", attributes);
    await this.db.silent(() =>
      this.db
        .system()
        .updateDocument(Base.METADATA, collection.getId(), collection),
    );

    this.db.trigger(EventsEnum.AttributeUpdate, collection, attributes[index]!);

    return attributes[index]!;
  }

  /**
   * Update required status of attribute.
   */
  public async updateAttributeRequired(
    collectionId: string,
    id: string,
    required: boolean,
  ): Promise<Doc<Attribute>> {
    return this.updateAttributeMeta(collectionId, id, (attribute) => {
      attribute.set("required", required);
    });
  }

  /**
   * Update format of attribute.
   */
  public async updateAttributeFormat(
    collectionId: string,
    id: string,
    format: string,
  ): Promise<Doc<Attribute>> {
    return this.updateAttributeMeta(collectionId, id, (attribute) => {
      if (!Structure.hasFormat(format, attribute.get("type"))) {
        throw new DatabaseException(
          `Format "${format}" not available for attribute type "${attribute.get("type")}"`,
        );
      }

      attribute.set("format", format);
    });
  }

  /**
   * Update format options of attribute.
   */
  public async updateAttributeFormatOptions(
    collectionId: string,
    id: string,
    formatOptions: Record<string, any>,
  ): Promise<Doc<Attribute>> {
    return this.updateAttributeMeta(collectionId, id, (attribute) => {
      attribute.set("formatOptions", formatOptions);
    });
  }

  /**
   * Update filters of attribute.
   */
  public async updateAttributeFilters(
    collectionId: string,
    id: string,
    filters: string[],
  ): Promise<Doc<Attribute>> {
    return this.updateAttributeMeta(collectionId, id, (attribute) => {
      attribute.set("filters", filters);
    });
  }

  /**
   * Update default value of attribute.
   */
  public async updateAttributeDefault(
    collectionId: string,
    id: string,
    defaultValue: any = null,
  ): Promise<Doc<Attribute>> {
    return this.updateAttributeMeta(collectionId, id, (attribute) => {
      if (attribute.get("required") === true) {
        throw new DatabaseException(
          "Cannot set a default value on a required attribute",
        );
      }

      this.internals.validateDefaultTypes(
        attribute.get("type"),
        defaultValue,
      );

      attribute.set("default", defaultValue);
    });
  }

  /**
   * Update an attribute in a collection.
   */
  public async updateAttribute(
    collectionId: string,
    id: string,
    options: {
      type?: AttributeEnum;
      size?: number;
      required?: boolean;
      default?: any;
      array?: boolean;
      format?: string;
      formatOptions?: Record<string, any>;
      filters?: string[];
      newKey?: string;
    } = {},
  ): Promise<Doc<Attribute>> {
    return this.updateAttributeMeta(
      collectionId,
      id,
      async (attribute, collection, attributeIndex) => {
        const {
          type = attribute.get("type"),
          size = attribute.get("size"),
          required = attribute.get("required"),
          default: defaultValue = attribute.get("default"),
          array = attribute.get("array"),
          format = attribute.get("format"),
          formatOptions = attribute.get("formatOptions"),
          filters = attribute.get("filters"),
          newKey,
        } = options;

        const altering =
          options.type !== undefined ||
          options.size !== undefined ||
          options.array !== undefined ||
          options.newKey !== undefined;

        const finalDefault =
          required === true && defaultValue !== null ? null : defaultValue;

        switch (type) {
          case AttributeEnum.String:
            if (!size) {
              throw new DatabaseException("Size length is required");
            }
            if (size > this.adapter.$limitForString) {
              throw new DatabaseException(
                `Max size allowed for string is: ${this.adapter.$limitForString}`,
              );
            }
            break;

          case AttributeEnum.Integer:
            if (size && size > this.adapter.$limitForInt) {
              throw new DatabaseException(
                `Max size allowed for int is: ${this.adapter.$limitForInt}`,
              );
            }
            break;

          case AttributeEnum.Float:
          case AttributeEnum.Boolean:
          case AttributeEnum.Json:
          case AttributeEnum.Uuid:
          case AttributeEnum.Timestamptz:
            if (size) {
              throw new DatabaseException("Size must be empty");
            }
            break;
          default:
            throw new DatabaseException(`Unknown attribute type: ${type}`);
        }

        if (format && !Structure.hasFormat(format, type)) {
          throw new DatabaseException(
            `Format "${format}" not available for attribute type "${type}"`,
          );
        }

        // Validate default value
        if (finalDefault !== null) {
          if (required) {
            throw new DatabaseException(
              "Cannot set a default value on a required attribute",
            );
          }
          this.internals.validateDefaultTypes(type, finalDefault);
        }

        const updatedId = newKey ?? id;
        attribute
          .set("$id", updatedId)
          .set("key", updatedId)
          .set("type", type)
          .set("size", size)
          .set("array", array)
          .set("format", format)
          .set("formatOptions", formatOptions)
          .set("filters", filters)
          .set("required", required)
          .set("default", finalDefault);

        const attributes = collection.get("attributes", []);
        attributes[attributeIndex] = attribute;
        collection.set("attributes", attributes);

        if (
          this.adapter.$documentSizeLimit > 0 &&
          this.adapter.getAttributeWidth(collection) >=
            this.adapter.$documentSizeLimit
        ) {
          throw new LimitException(
            "Row width limit reached. Cannot update attribute.",
          );
        }

        if (altering) {
          const indexes = collection.get("indexes", []);

          // Update index attribute references if key changed
          if (newKey && id !== newKey) {
            indexes.forEach((index) => {
              const indexAttributes = index.get("attributes", []);
              if (indexAttributes.includes(id)) {
                const updatedAttributes = indexAttributes.map((attr) =>
                  attr === id ? newKey : attr,
                );
                index.set("attributes", updatedAttributes);
              }
            });
          }

          if (this.internals.validate()) {
            const validator = new IndexValidator(
              attributes,
              this.adapter.$maxIndexLength,
              this.adapter.$internalIndexesKeys,
              this.adapter.$supportForIndexArray,
            );

            indexes.forEach((index) => {
              if (!validator.$valid(index)) {
                throw new IndexException(validator.$description);
              }
            });
          }

          await this.adapter.updateAttribute({
            key: id,
            collection: collectionId,
            type,
            size,
            array,
            newName: newKey,
          });
          await this.db.purgeCachedCollection(collection);
        }

        await this.db.purgeCachedDocument(Base.METADATA, collection);
      },
    );
  }

  /**
   * Deletes an attribute from a collection.
   */
  public async deleteAttribute(
    collectionId: string,
    attributeId: string,
  ): Promise<boolean> {
    const collection = await this.db.silent(() =>
      this.getCollection(collectionId),
    );

    if (collection.getId() === Base.METADATA) {
      throw new DatabaseException("Cannot delete metadata attributes");
    }

    const attributes = collection.get("attributes", []);
    const indexes = collection.get("indexes", []);

    const attributeIndex = attributes.findIndex(
      (attr: Doc<Attribute>) => attr.get("$id") === attributeId,
    );
    if (attributeIndex === -1) {
      throw new NotFoundException("Attribute not found");
    }

    const attribute = attributes[attributeIndex]!;
    if (attribute.get("type") === AttributeEnum.Relationship) {
      throw new DatabaseException("Cannot delete relationship as an attribute");
    }
    if (attribute.get("type") === AttributeEnum.Virtual) {
      throw new DatabaseException("Cannot delete virtual attribute");
    }

    if (this.internals.validate()) {
      const validator = new IndexDependency(
        indexes,
        this.adapter.$supportForCastIndexArray,
      );

      if (!validator.$valid(attribute)) {
        throw new DependencyException(validator.$description);
      }
    }

    // Remove attribute from indexes
    for (const index of indexes) {
      const indexAttributes = index.get("attributes", []);
      const updatedAttributes = indexAttributes.filter(
        (attr) => attr !== attributeId,
      );

      if (updatedAttributes.length === 0) {
        indexes.splice(indexes.indexOf(index), 1);
      } else {
        index.set("attributes", updatedAttributes);
      }
    }

    // Remove attribute from collection
    attributes.splice(attributeIndex, 1);
    collection.set("attributes", attributes);
    collection.set("indexes", indexes);

    try {
      await this.adapter.deleteAttribute(collection.getId(), attributeId);
    } catch (error) {
      if (!(error instanceof NotFoundException)) {
        throw error;
      }
    }

    if (collection.getId() !== Base.METADATA) {
      await this.db.silent(() =>
        this.db
          .system()
          .updateDocument(Base.METADATA, collection.getId(), collection),
      );
    }

    await this.db.purgeCachedCollection(collection);
    await this.db.purgeCachedDocument(Base.METADATA, collection);

    this.db.trigger(EventsEnum.AttributeDelete, collection, attribute);

    return true;
  }

  /**
   * Renames an attribute in a collection.
   */
  public async renameAttribute(
    collectionId: string,
    oldName: string,
    newName: string,
  ): Promise<boolean> {
    const collection = await this.db.silent(() =>
      this.getCollection(collectionId),
    );

    if (collection.empty()) {
      throw new NotFoundException(`Collection '${collectionId}' not found`);
    }

    const attributes = collection.get("attributes", []);
    const indexes = collection.get("indexes", []);

    const attribute = attributes.find((attr) => attr.get("$id") === oldName);
    if (!attribute) {
      throw new NotFoundException(`Attribute '${oldName}' not found`);
    }

    if (attributes.some((attr) => attr.get("$id") === newName)) {
      throw new DuplicateException(`Attribute name '${newName}' already used`);
    }

    if (this.internals.validate()) {
      const validator = new IndexDependency(
        indexes,
        this.adapter.$supportForCastIndexArray,
      );

      if (!validator.$valid(attribute)) {
        throw new DependencyException(validator.$description);
      }
    }

    attribute.set("$id", newName);
    attribute.set("key", newName);

    for (const index of indexes) {
      const indexAttributes = index.get("attributes", []);
      const updatedAttributes = indexAttributes.map((attr) =>
        attr === oldName ? newName : attr,
      );
      index.set("attributes", updatedAttributes);
    }

    await this.adapter.renameAttribute(collection.getId(), oldName, newName);

    collection.set("attributes", attributes);
    collection.set("indexes", indexes);

    if (collection.getId() !== Base.METADATA) {
      await this.db.silent(() =>
        this.db
          .system()
          .updateDocument(Base.METADATA, collection.getId(), collection),
      );
    }

    this.db.trigger(EventsEnum.AttributeUpdate, collection, attribute);

    return true;
  }

  /**
   * Creates a relationship between two collections.
   */
  public async createRelationship({
    collectionId,
    relatedCollectionId,
    type,
    twoWay = false,
    id,
    twoWayKey,
    onDelete = OnDelete.Restrict,
  }: CreateRelationshipAttribute): Promise<boolean> {
    return createRelationship(this.db, {
      collectionId,
      relatedCollectionId,
      type,
      twoWay,
      id,
      twoWayKey,
      onDelete,
    });
  }

  /**
   * Updates an existing relationship in a collection.
   */
  public async updateRelationship({
    collectionId,
    id,
    newKey,
    newTwoWayKey,
    twoWay,
    onDelete,
  }: UpdateRelationshipAttribute): Promise<boolean> {
    return updateRelationship(this.db, {
      collectionId,
      id,
      newKey,
      newTwoWayKey,
      twoWay,
      onDelete,
    });
  }

  /**
   * Deletes a relationship between two collections.
   */
  public async deleteRelationship(
    collectionId: string,
    id: string,
  ): Promise<boolean> {
    return deleteRelationship(this.db, collectionId, id);
  }

  /**
   * Renames an index in a collection.
   */
  public async renameIndex(
    collectionId: string,
    oldName: string,
    newName: string,
  ): Promise<boolean> {
    return renameIndex(this.db, collectionId, oldName, newName);
  }

  /**
   * Creates an index in a collection.
   */
  public async createIndex(
    collectionId: string,
    id: string,
    type: string,
    attributes: string[],
    orders: (string | null)[] = [],
  ): Promise<boolean> {
    return createIndex(
      this.db,
      this.internals.validate(),
      collectionId,
      id,
      type,
      attributes,
      orders,
    );
  }

  /**
   * Delete an index in a collection.
   */
  public async deleteIndex(
    collectionId: string,
    id: string,
  ): Promise<boolean> {
    return deleteIndex(this.db, collectionId, id);
  }
}
