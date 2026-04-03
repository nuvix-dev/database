import {
  AttributeEnum,
  EventsEnum,
  IndexEnum,
  OnDelete,
  PermissionEnum,
  RelationEnum,
  RelationSideEnum,
} from "./enums.js";
import {
  Attribute,
  Collection,
  Index,
  RelationOptions,
} from "@validators/schema.js";
import {
  CreateCollection,
  CreateRelationshipAttribute,
  Filters,
  QueryByType,
  UpdateCollection,
  UpdateRelationshipAttribute,
} from "./types.js";
import { Cache } from "./cache.js";
import { Cache as NuvixCache } from "@nuvix/cache";
import { Entities, IEntity } from "types.js";
import { QueryBuilder } from "@utils/query-builder.js";
import { Query } from "./query.js";
import { Doc } from "./doc.js";
import {
  AuthorizationException,
  ConflictException,
  DatabaseException,
  DependencyException,
  DuplicateException,
  IndexException,
  LimitException,
  NotFoundException,
  QueryException,
  RelationshipException,
  StructureException,
} from "@errors/index.js";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";
import { Permissions } from "@validators/permissions.js";
import { Index as IndexValidator } from "@validators/index-validator.js";
import { Documents } from "@validators/queries/documents.js";
import { Authorization } from "@utils/authorization.js";
import { ID } from "@utils/id.js";
import { Structure } from "@validators/structure.js";
import { Adapter } from "@adapters/adapter.js";
import { IndexDependency } from "@validators/index-dependency.js";
import { MethodType } from "@validators/query/base.js";

export class Database extends Cache {
  constructor(
    adapter: Adapter,
    cache: NuvixCache,
    options: DatabaseOptions = {},
  ) {
    super(adapter, cache, options);
  }

  /**
   * Creates a new database.
   */
  public async create(database?: string): Promise<void> {
    database = database ?? this.adapter.$schema;
    await this.adapter.create(database);

    const attributes = [...Database.COLLECTION.attributes].map(
      (attr) => new Doc(attr),
    );
    await this.silent(() =>
      this.createCollection({ id: Database.METADATA, attributes }),
    );

    this.trigger(EventsEnum.DatabaseCreate, database);
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
    this.trigger(EventsEnum.DatabaseList, []);
    return [];
  }

  /**
   * Delete a database.
   */
  public async delete(database?: string): Promise<void> {
    database ??= this.adapter.$schema;
    await this.adapter.delete(database);

    this.trigger(EventsEnum.DatabaseDelete, database);
    await this.cache.flush();
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

    if (this.validate) {
      const perms = new Permissions();
      if (!perms.$valid(permissions)) {
        throw new DatabaseException(perms.$description);
      }
    }

    let collection = await this.silent(() => this.getCollection(id));
    if (!collection.empty() && id !== Database.METADATA) {
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

    if (this.validate) {
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
        if (!this.sharedTables || !this.migrating) {
          throw error;
        }
      } else {
        throw error;
      }
    }

    if (id === Database.METADATA) return new Doc(Database.COLLECTION);

    const createdCollection = await this.silent(() =>
      this.createDocument(Database.METADATA, collection),
    );
    this.trigger(EventsEnum.CollectionCreate, createdCollection);

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
      if (this.validate) {
        const perms = new Permissions();
        if (!perms.$valid(permissions)) {
          throw new DatabaseException(perms.$description);
        }
      }
    }

    let collection = await this.silent(() => this.getCollection(id, true));

    if (
      this.adapter.$sharedTables &&
      collection.getTenant() !== this.adapter.$tenantId
    ) {
      throw new NotFoundException(`Collection '${id}' not found`);
    }

    collection.set("$permissions", permissions);
    collection.set("documentSecurity", documentSecurity);
    collection.set("enabled", enabled);

    collection = await this.silent(() =>
      this.updateDocument(Database.METADATA, collection.getId(), collection),
    );
    this.trigger(EventsEnum.CollectionUpdate, collection);

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
    let collection = await this.silent(() =>
      this.getDocument<Collection>(Database.METADATA, id),
    );

    if (
      id !== Database.METADATA &&
      this.adapter.$sharedTables &&
      collection.getTenant() !== null &&
      collection.getTenant() !== this.adapter.$tenantId
    ) {
      if (throwOnNotFound) {
        throw new NotFoundException(`Collection '${id}' not found`);
      }
      return new Doc<Collection>();
    }

    if (this.assertCollectionEnabled(collection)) {
      collection = new Doc<Collection>();
    }

    this.trigger(EventsEnum.CollectionRead, collection);
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

    return this.find<Collection>(Database.METADATA, query);
  }

  /**
   * Gets the size of a collection.
   */
  public async getSizeOfCollection(collectionId: string): Promise<number> {
    const collection = await this.silent(() =>
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

    const collection = await this.silent(() =>
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
    const collection = await this.silent(() =>
      this.getDocument(Database.METADATA, id),
    );

    if (collection.empty() || collection.getId() === Database.METADATA) {
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
        (attribute) => attribute.get("type") === AttributeEnum.Relationship,
      );

    return await this.withTransaction(async (db) => {
      for (const relationship of relationships) {
        await db.deleteRelationship(
          collection.getId(),
          relationship.get("$id"),
        );
      }

      try {
        await db.adapter.deleteCollection(id);
      } catch (error) {
        if (error instanceof NotFoundException) {
          // HACK: Metadata should still be updated, can be removed when null tenant collections are supported.
          if (!db.adapter.$sharedTables || !this.migrating) {
            throw error;
          }
        } else {
          throw error;
        }
      }

      let deleted: boolean;
      if (id === Database.METADATA) {
        deleted = true;
      } else {
        deleted = await db.silent(() =>
          db.deleteDocument(Database.METADATA, id),
        );
      }

      if (deleted) {
        // todo:
        this.trigger(EventsEnum.CollectionDelete, collection);
      }

      await this.purgeCachedCollection(id);

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

    let collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );
    const attr = await this.validateAttribute(collection, attribute);

    collection.append("attributes", attr);

    try {
      await this.adapter.createAttribute({
        collection: collectionId,
        ...attribute,
      });
    } catch (error) {
      if (error instanceof DuplicateException) {
        // HACK: Metadata should still be updated, can be removed when null tenant collections are supported.
        if (!this.adapter.$sharedTables || !this.migrating) {
          throw error;
        }
      } else throw error;
    }

    if (collection.getId() !== Database.METADATA) {
      collection = await this.silent(() =>
        this.updateDocument(Database.METADATA, collection.getId(), collection),
      );
    }

    this.trigger(EventsEnum.AttributeCreate, collection, attr);
    return true;
  }

  /**
   * Creates multiple attributes in a collection.
   */
  public async createAttributes(collectionId: string, attributes: Attribute[]) {
    if (attributes.length === 0) {
      throw new DatabaseException("No attributes to create");
    }

    let collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );
    const attrDocs: Doc<Attribute>[] = [];

    for (const attribute of attributes) {
      const attr = await this.validateAttribute(collection, attribute);

      collection.append("attributes", attr);
      attrDocs.push(attr);
    }

    try {
      await this.adapter.createAttributes(collection.getId(), attributes);
    } catch (error) {
      if (error instanceof DuplicateException) {
        // No attributes were in a metadata, but at least one of them was present on the table
        // HACK: Metadata should still be updated, can be removed when null tenant collections are supported.
        if (!this.adapter.$sharedTables || !this.migrating) {
          throw error;
        }
      }
      throw error;
    }

    if (collection.getId() !== Database.METADATA) {
      collection = await this.silent(() =>
        this.updateDocument(Database.METADATA, collection.getId(), collection),
      );
    }

    this.purgeCachedCollection(collection);
    this.purgeCachedDocument(Database.METADATA, collection);

    this.trigger(EventsEnum.AttributesCreate, collection, attrDocs);
    return true;
  }

  /**
   * Update index metadata. Utility method for update index methods.
   */
  protected async updateIndexMeta(
    collectionId: string,
    id: string,
    updateCallback: (
      index: Doc<Index>,
      collection: Doc<Collection>,
      indexPosition: number,
    ) => void,
  ): Promise<Doc<Index>> {
    let collection = await this.silent(() => this.getCollection(collectionId));

    if (collection.getId() === Database.METADATA) {
      throw new DatabaseException("Cannot update metadata indexes");
    }

    const indexes = collection.get("indexes", []);
    const indexPosition = indexes.findIndex(
      (index: Doc<Index>) => index.get("$id") === id,
    );

    if (indexPosition === -1) {
      throw new NotFoundException("Index not found");
    }

    // Execute update from callback
    updateCallback(indexes[indexPosition]!, collection, indexPosition);

    // Save
    collection.set("indexes", indexes);
    await this.silent(() =>
      this.updateDocument(Database.METADATA, collection.getId(), collection),
    );

    this.trigger(
      EventsEnum.AttributeUpdate,
      collection,
      indexes[indexPosition]!,
    );

    return indexes[indexPosition]!;
  }

  /**
   * Update attribute metadata. Utility method for update attribute methods.
   */
  protected async updateAttributeMeta(
    collectionId: string,
    id: string,
    updateCallback: (
      attribute: Doc<Attribute>,
      collection: Doc<Collection>,
      index: number,
    ) => void | Promise<void>,
  ): Promise<Doc<Attribute>> {
    let collection = await this.silent(() => this.getCollection(collectionId));

    if (collection.getId() === Database.METADATA) {
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
    await this.silent(() =>
      this.updateDocument(Database.METADATA, collection.getId(), collection),
    );

    this.trigger(EventsEnum.AttributeUpdate, collection, attributes[index]!);

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

      this.validateDefaultTypes(attribute.get("type"), defaultValue);

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
          this.validateDefaultTypes(type, finalDefault);
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

          if (this.validate) {
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
          await this.purgeCachedCollection(collection);
        }

        await this.purgeCachedDocument(Database.METADATA, collection);
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
    const collection = await this.silent(() =>
      this.getCollection(collectionId),
    );

    if (collection.getId() === Database.METADATA) {
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

    if (this.validate) {
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

    if (collection.getId() !== Database.METADATA) {
      await this.silent(() =>
        this.updateDocument(Database.METADATA, collection.getId(), collection),
      );
    }

    await this.purgeCachedCollection(collection);
    await this.purgeCachedDocument(Database.METADATA, collection);

    this.trigger(EventsEnum.AttributeDelete, collection, attribute);

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
    const collection = await this.silent(() =>
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

    if (this.validate) {
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

    if (collection.getId() !== Database.METADATA) {
      await this.silent(() =>
        this.updateDocument(Database.METADATA, collection.getId(), collection),
      );
    }

    this.trigger(EventsEnum.AttributeUpdate, collection, attribute);

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
    const collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );
    const relatedCollection = await this.silent(() =>
      this.getCollection(relatedCollectionId),
    );

    if (relatedCollection.empty()) {
      throw new NotFoundException("Related collection not found");
    }

    id ??= relatedCollection.getId();
    twoWayKey ??= collection.getId();

    const attributes = collection.get("attributes", []);
    for (const attribute of attributes) {
      if (attribute.get("$id").toLowerCase() === id.toLowerCase()) {
        throw new DuplicateException("Attribute already exists");
      }

      const options = attribute.get("options", {});
      if (
        attribute.get("type") === AttributeEnum.Relationship &&
        options.twoWayKey?.toLowerCase() === twoWayKey.toLowerCase() &&
        options.relatedCollection === relatedCollection.getId()
      ) {
        throw new DuplicateException("Related attribute already exists");
      }
    }

    const relationship = new Doc<Attribute>({
      $id: id,
      key: id,
      type: AttributeEnum.Relationship,
      required: false,
      default: null,
      options: {
        relatedCollection: relatedCollection.getId(),
        relationType: type,
        twoWay: twoWay,
        twoWayKey: twoWayKey,
        onDelete: onDelete,
        side: RelationSideEnum.Parent,
      },
    });

    const twoWayRelationship = new Doc<Attribute>({
      $id: twoWayKey,
      key: twoWayKey,
      type: AttributeEnum.Relationship,
      required: false,
      default: null,
      options: {
        relatedCollection: collection.getId(),
        relationType: type,
        twoWay: twoWay,
        twoWayKey: id,
        onDelete: onDelete,
        side: RelationSideEnum.Child,
      },
    });

    this.checkAttribute(collection, relationship);
    this.checkAttribute(relatedCollection, twoWayRelationship);

    collection.append("attributes", relationship);
    relatedCollection.append("attributes", twoWayRelationship);

    if (type === RelationEnum.ManyToMany) {
      const junctionCollectionName = this.getJunctionTable(
        collection.getSequence(),
        relatedCollection.getSequence(),
        relationship.getId(),
        twoWayRelationship.getId(),
      );
      await this.silent(() =>
        this.createCollection({
          id: junctionCollectionName,
          attributes: [
            new Doc<Attribute>({
              $id: id,
              key: id,
              type: AttributeEnum.String,
              size: this.adapter.$limitForString,
              required: true,
            }),
            new Doc<Attribute>({
              $id: twoWayKey,
              key: twoWayKey,
              type: AttributeEnum.String,
              size: this.adapter.$limitForString,
              required: true,
            }),
          ],
          indexes: [
            new Doc<Index>({
              $id: `_index_${id}`,
              key: `_index_${id}`,
              type: IndexEnum.Key,
              attributes: [id],
            }),
            new Doc<Index>({
              $id: `_index_${twoWayKey}`,
              key: `_index_${twoWayKey}`,
              type: IndexEnum.Key,
              attributes: [twoWayKey],
            }),
          ],
        }),
      );
    }

    const created = await this.adapter.createRelationship(
      collection.getId(),
      relatedCollection.getId(),
      type,
      twoWay,
      id,
      twoWayKey,
    );

    if (!created) {
      throw new DatabaseException("Failed to create relationship");
    }

    await this.silent(async () => {
      try {
        await this.withTransaction(async (db) => {
          await db.updateDocument(
            Database.METADATA,
            collection.getId(),
            collection,
          );
          await db.updateDocument(
            Database.METADATA,
            relatedCollection.getId(),
            relatedCollection,
          );
        });
      } catch (error: any) {
        try {
          await this.silent(() => {
            if (type === RelationEnum.ManyToMany) {
              // If the relationship is ManyToMany, we need to delete the junction collection
              return this.adapter.deleteCollection(
                this.getJunctionTable(
                  collection.getSequence(),
                  relatedCollection.getSequence(),
                  id,
                  twoWayKey,
                ),
              ) as any;
            } else
              return this.adapter.deleteRelationship(
                collection.getId(),
                relatedCollection.getId(),
                type,
                twoWay,
                id,
                twoWayKey,
                RelationSideEnum.Parent,
              );
          });
        } catch {}
        throw new DatabaseException(
          `Failed to create relationship: ${error.message}`,
        );
      }

      const indexKey = `_index_${id}`;
      const twoWayIndexKey = `_index_${twoWayKey}`;

      switch (type) {
        case RelationEnum.OneToOne:
          await this.createIndex(
            collection.getId(),
            indexKey,
            IndexEnum.Unique,
            [id],
          );
          if (twoWay) {
            await this.createIndex(
              relatedCollection.getId(),
              twoWayIndexKey,
              IndexEnum.Unique,
              [twoWayKey],
            );
          }
          break;
        case RelationEnum.OneToMany:
          await this.createIndex(
            relatedCollection.getId(),
            twoWayIndexKey,
            IndexEnum.Key,
            [twoWayKey],
          );
          break;
        case RelationEnum.ManyToOne:
          await this.createIndex(collection.getId(), indexKey, IndexEnum.Key, [
            id,
          ]);
          break;
        case RelationEnum.ManyToMany:
          // Indexes are created during junction collection creation
          break;
        default:
          throw new DatabaseException("Invalid relationship type.");
      }
    });

    this.trigger(
      EventsEnum.RelationshipCreate,
      collection,
      relationship,
      relatedCollection,
      twoWayRelationship,
    );

    return true;
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
    if (!newKey && !newTwoWayKey && twoWay === undefined && !onDelete) {
      return true;
    }

    const collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );
    const attributes = collection.get("attributes", []);

    if (newKey && attributes.some((attr) => attr.get("key") === newKey)) {
      throw new DuplicateException("Relationship already exists");
    }

    const attributeIndex = attributes.findIndex(
      (attr) => attr.get("$id") === id,
    );
    if (attributeIndex === -1) {
      throw new NotFoundException("Relationship not found");
    }

    const attribute = attributes[attributeIndex]!;
    const type = attribute.get("options")["relationType"];
    const side = attribute.get("options")["side"];

    if (type === RelationEnum.ManyToMany && (newTwoWayKey || newKey)) {
      throw new DatabaseException("Cannot update ManyToMany relationship.");
    }

    const relatedCollectionId = attribute.get("options")["relatedCollection"];
    const relatedCollection = await this.silent(() =>
      this.getCollection(relatedCollectionId),
    );

    await this.updateAttributeMeta(collection.getId(), id, async (attr) => {
      const altering =
        (newKey && newKey !== id) ||
        (newTwoWayKey && newTwoWayKey !== attr.get("options")["twoWayKey"]);

      const relatedAttributes = relatedCollection.get("attributes", []);
      if (
        newTwoWayKey &&
        relatedAttributes.some((attr) => attr.get("key") === newTwoWayKey)
      ) {
        throw new DuplicateException("Related attribute already exists");
      }

      newKey ??= attr.get("key");
      const twoWayKey = attr.get("options")["twoWayKey"];
      newTwoWayKey ??= twoWayKey;
      twoWay ??= attr.get("options")["twoWay"];
      onDelete ??= attr.get("options.onDelete");

      attr.set("$id", newKey).set("key", newKey).set("options", {
        relatedCollection: relatedCollection.getId(),
        relationType: type,
        twoWay,
        twoWayKey: newTwoWayKey,
        onDelete,
        side,
      });

      await this.updateAttributeMeta(
        relatedCollection.getId(),
        twoWayKey,
        (relatedAttr) => {
          relatedAttr
            .set("$id", newTwoWayKey)
            .set("key", newTwoWayKey)
            .set("options", {
              ...relatedAttr.get("options"),
              twoWayKey: newKey,
              twoWay,
              onDelete,
            });
        },
      );

      // if (type === RelationEnum.ManyToMany) {
      //     const junction = this.getJunctionTable(
      //         collection.getSequence(),
      //         relatedCollection.getSequence(),
      //         id,
      //         twoWayKey
      //     );

      //     await this.renameAttribute(junction, id, newKey);
      //     newTwoWayKey !== undefined && await this.renameAttribute(junction, twoWayKey, newTwoWayKey);
      //     await this.purgeCachedCollection(junction);
      // }

      if (altering) {
        const updated = await this.adapter.updateRelationship(
          collection.getId(),
          relatedCollection.getId(),
          type,
          twoWay,
          id,
          twoWayKey,
          side,
          newKey,
          newTwoWayKey,
        );

        if (!updated) {
          throw new DatabaseException("Failed to update relationship");
        }
      }
    });

    const renameIndex = async (
      collectionId: string,
      key: string,
      newKey: string,
    ) => {
      await this.updateIndexMeta(collectionId, `_index_${key}`, (index) => {
        index.set("attributes", [newKey]);
      });
      await this.silent(() =>
        this.renameIndex(collectionId, `_index_${key}`, `_index_${newKey}`),
      );
    };

    newKey ??= attribute.get("key");
    const twoWayKey: string = attribute.get("options")["twoWayKey"];
    newTwoWayKey ??= twoWayKey;

    switch (type) {
      case RelationEnum.OneToOne:
        if (id !== newKey) {
          await renameIndex(collection.getId(), id, newKey);
        }
        if (twoWay && twoWayKey !== newTwoWayKey) {
          await renameIndex(relatedCollection.getId(), twoWayKey, newTwoWayKey);
        }
        break;
      case RelationEnum.OneToMany:
        if (side === RelationSideEnum.Parent) {
          if (twoWayKey !== newTwoWayKey) {
            await renameIndex(
              relatedCollection.getId(),
              twoWayKey,
              newTwoWayKey,
            );
          }
        } else {
          if (id !== newKey) {
            await renameIndex(collection.getId(), id, newKey);
          }
        }
        break;
      case RelationEnum.ManyToOne:
        if (side === RelationSideEnum.Parent) {
          if (id !== newKey) {
            await renameIndex(collection.getId(), id, newKey);
          }
        } else {
          if (twoWayKey !== newTwoWayKey) {
            await renameIndex(
              relatedCollection.getId(),
              twoWayKey,
              newTwoWayKey,
            );
          }
        }
        break;
      case RelationEnum.ManyToMany:
        // const junction = this.getJunctionTable(
        //     collection.getSequence(),
        //     relatedCollection.getSequence(),
        //     id,
        //     twoWayKey
        // );

        // if (id !== newKey) {
        //     await renameIndex(junction, id, newKey);
        // }
        // if (twoWayKey !== newTwoWayKey) {
        //     await renameIndex(junction, twoWayKey, newTwoWayKey);
        // }
        break;
      default:
        throw new DatabaseException("Invalid relationship type.");
    }

    await this.purgeCachedCollection(collection.getId());
    await this.purgeCachedCollection(relatedCollection.getId());

    return true;
  }

  /**
   * Deletes a relationship between two collections.
   */
  public async deleteRelationship(
    collectionId: string,
    id: string,
  ): Promise<boolean> {
    const collection = await this.silent(() =>
      this.getCollection(collectionId),
    );
    const attributes = collection.get("attributes", []);
    let relationship: Doc<Attribute> | null = null;
    let relationshipIndex = -1;

    for (let i = 0; i < attributes.length; i++) {
      if (attributes[i]!.get("$id") === id) {
        relationship = attributes[i]!;
        relationshipIndex = i;
        break;
      }
    }

    if (!relationship) {
      throw new NotFoundException("Relationship not found");
    }

    // Remove relationship from collection attributes
    attributes.splice(relationshipIndex, 1);
    collection.set("attributes", attributes);

    const options = relationship.get("options", {}) as RelationOptions;
    const relatedCollectionId = options.relatedCollection;
    const type = options.relationType;
    const twoWay = Boolean(options.twoWay);
    const twoWayKey = options.twoWayKey;
    const side = options.side;

    const relatedCollection = await this.silent(() =>
      this.getCollection(relatedCollectionId),
    );
    const relatedAttributes = relatedCollection.get("attributes", []);

    // Remove two-way relationship from related collection
    const updatedRelatedAttributes = relatedAttributes.filter(
      (attr) => attr.get("$id") !== twoWayKey,
    );
    relatedCollection.set("attributes", updatedRelatedAttributes);

    await this.silent(async () => {
      try {
        await this.withTransaction(async (db) => {
          await db.updateDocument(
            Database.METADATA,
            collection.getId(),
            collection,
          );
          await db.updateDocument(
            Database.METADATA,
            relatedCollection.getId(),
            relatedCollection,
          );
        });
      } catch (error: any) {
        throw new DatabaseException(
          `Failed to delete relationship: ${error.message}`,
        );
      }

      const indexKey = `_index_${id}`;
      const twoWayIndexKey = `_index_${twoWayKey}`;

      switch (type) {
        case RelationEnum.OneToOne:
          if (side === RelationSideEnum.Parent) {
            await this.deleteIndex(collection.getId(), indexKey);
            if (twoWay) {
              await this.deleteIndex(relatedCollection.getId(), twoWayIndexKey);
            }
          }
          if (side === RelationSideEnum.Child) {
            await this.deleteIndex(relatedCollection.getId(), twoWayIndexKey);
            if (twoWay) {
              await this.deleteIndex(collection.getId(), indexKey);
            }
          }
          break;
        case RelationEnum.OneToMany:
          if (side === RelationSideEnum.Parent) {
            await this.deleteIndex(relatedCollection.getId(), twoWayIndexKey);
          } else {
            await this.deleteIndex(collection.getId(), indexKey);
          }
          break;
        case RelationEnum.ManyToOne:
          if (side === RelationSideEnum.Parent) {
            await this.deleteIndex(collection.getId(), indexKey);
          } else {
            await this.deleteIndex(relatedCollection.getId(), twoWayIndexKey);
          }
          break;
        case RelationEnum.ManyToMany:
          const junctionCollectionName = this.getJunctionTable(
            collection.getSequence(),
            relatedCollection.getSequence(),
            id,
            twoWayKey!,
          );
          await this.deleteCollection(junctionCollectionName);
          break;
        default:
          throw new RelationshipException("Invalid relationship type.");
      }
    });

    const deleted = await this.adapter.deleteRelationship(
      collection.getId(),
      relatedCollection.getId(),
      type,
      twoWay,
      id,
      twoWayKey!,
      side,
    );

    if (!deleted) {
      throw new DatabaseException("Failed to delete relationship");
    }

    await this.purgeCachedCollection(collection.getId());
    await this.purgeCachedCollection(relatedCollection.getId());

    this.trigger(EventsEnum.AttributeDelete, collection, relationship);

    return true;
  }

  /**
   * Renames an index in a collection.
   */
  public async renameIndex(
    collectionId: string,
    oldName: string,
    newName: string,
  ): Promise<boolean> {
    const collection = await this.silent(() =>
      this.getCollection(collectionId),
    );

    if (collection.empty()) {
      throw new NotFoundException(`Collection '${collectionId}' not found`);
    }

    const indexes = collection.get("indexes", []);
    const index = indexes.find((idx: Doc<Index>) => idx.get("$id") === oldName);

    if (!index) {
      throw new NotFoundException(`Index '${oldName}' not found`);
    }

    if (indexes.some((idx: Doc<Index>) => idx.get("$id") === newName)) {
      throw new DuplicateException(`Index name '${newName}' already used`);
    }

    index.set("$id", newName);
    index.set("key", newName);

    collection.set("indexes", indexes);

    await this.adapter.renameIndex(collection.getId(), oldName, newName);

    if (collection.getId() !== Database.METADATA) {
      await this.silent(() =>
        this.updateDocument(Database.METADATA, collection.getId(), collection),
      );
    }

    this.trigger(EventsEnum.IndexRename, collection, index, oldName);

    return true;
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
    if (attributes.length === 0) {
      throw new DatabaseException("Missing attributes");
    }

    const collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );

    const indexes = collection.get("indexes", []);
    if (
      indexes.some(
        (index: Doc<Index>) =>
          index.get("$id").toLowerCase() === id.toLowerCase(),
      )
    ) {
      throw new DuplicateException("Index already exists");
    }

    if (
      this.adapter.getCountOfIndexes(collection) >=
      this.adapter.$limitForIndexes
    ) {
      throw new LimitException("Index limit reached. Cannot create new index.");
    }

    switch (type) {
      case IndexEnum.Key:
        if (!this.adapter.$supportForIndex) {
          throw new DatabaseException("Key index is not supported");
        }
        break;
      case IndexEnum.Unique:
        if (!this.adapter.$supportForUniqueIndex) {
          throw new DatabaseException("Unique index is not supported");
        }
        break;
      case IndexEnum.FullText:
        if (!this.adapter.$supportForFulltextIndex) {
          throw new DatabaseException("Fulltext index is not supported");
        }
        break;
      default:
        throw new DatabaseException(
          `Unknown index type: ${type}. Must be one of [${Object.values(IndexEnum).join(", ")}]`,
        );
    }

    const collectionAttributes = collection.get("attributes", []);
    const indexAttributesWithTypes: Record<string, Attribute> = {};

    attributes.forEach((attr, i) => {
      const collectionAttribute = collectionAttributes.find(
        (attribute: Doc<Attribute>) => attribute.get("key") === attr,
      );
      if (!collectionAttribute) {
        throw new DatabaseException(
          `Attribute '${attr}' not found in collection '${collectionId}'`,
        );
      }

      indexAttributesWithTypes[attr] = collectionAttribute.toObject();
      if (collectionAttribute.get("array", false)) {
        orders[i] = null;
      }
    });

    const index = new Doc<Index>({
      $id: id,
      key: id,
      type: type,
      attributes: attributes,
      orders: orders,
    });

    collection.append("indexes", index);

    if (this.validate) {
      const validator = new IndexValidator(
        collectionAttributes,
        this.adapter.$maxIndexLength,
        this.adapter.$internalIndexesKeys,
        this.adapter.$supportForIndexArray,
      );
      if (!validator.$valid(index)) {
        throw new IndexException(validator.$description);
      }
    }

    try {
      const created = await this.adapter.createIndex({
        collection: collection.getId(),
        name: id,
        type,
        attributes,
        orders,
        attributeTypes: indexAttributesWithTypes,
      });

      if (!created) {
        throw new DatabaseException("Failed to create index");
      }
    } catch (error) {
      if (error instanceof DuplicateException) {
        if (!this.adapter.$sharedTables || !this.migrating) {
          throw error;
        }
      } else {
        throw error;
      }
    }

    if (collection.getId() !== Database.METADATA) {
      await this.silent(() =>
        this.updateDocument(Database.METADATA, collection.getId(), collection),
      );
    }

    this.trigger(EventsEnum.IndexCreate, collection, index);

    return true;
  }

  /**
   * Delete an index in a collection.
   */
  public async deleteIndex(collectionId: string, id: string): Promise<boolean> {
    const collection = await this.silent(() =>
      this.getCollection(collectionId),
    );

    const indexes = collection.get("indexes", []);

    let indexDeleted: Doc<Index> | null = null;
    const updatedIndexes = indexes.filter((index: Doc<Index>) => {
      if (index.get("$id") === id) {
        indexDeleted = index;
        return false;
      }
      return true;
    });

    const deleted = await this.adapter.deleteIndex(collection.getId(), id);

    collection.set("indexes", updatedIndexes);

    if (collection.getId() !== Database.METADATA) {
      await this.silent(() =>
        this.updateDocument(Database.METADATA, collection.getId(), collection),
      );
    }

    this.trigger(EventsEnum.IndexDelete, collection, indexDeleted);

    return deleted;
  }

  /**
   * Get a document by ID.
   */
  public getDocument<C extends string & keyof Entities>(
    collectionId: C,
    id: string,
    query?: ((builder: QueryBuilder<C>) => QueryBuilder<C>) | Query[],
    forUpdate?: boolean,
  ): Promise<Doc<Entities[C]>>;
  public getDocument<C extends string>(
    collectionId: C,
    id: string,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    forUpdate?: boolean,
  ): Promise<Doc<Partial<IEntity> & Record<string, any>>>;
  public getDocument<D extends Record<string, any>>(
    collectionId: string,
    id: string,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    forUpdate?: boolean,
  ): Promise<Doc<Partial<IEntity> & D>>;
  public async getDocument(
    collectionId: string,
    id: string,
    query: ((builder: QueryBuilder) => QueryBuilder) | Query[] = [],
    forUpdate: boolean = false,
  ): Promise<any> {
    if (collectionId === Database.METADATA && id === Database.METADATA) {
      return new Doc(Database.COLLECTION);
    }

    if (!collectionId) {
      throw new NotFoundException(`Collection '${collectionId}' not found.`);
    }
    if (!id) {
      return new Doc();
    }

    const collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );
    const processedQuery = await this.processQueries(query, collection, {
      forUpdate,
      overrideValidators: [MethodType.Populate, MethodType.Select],
    });
    const processedQueryClone = { ...processedQuery };

    let doc: Doc<any>;
    const { collectionKey, documentKey, filtersHash } = this.getCacheKeys(
      collectionId,
      id,
      processedQueryClone,
    );
    const cacheKey = `${documentKey}${filtersHash ? ":" + filtersHash : ""}`;

    if (!processedQuery.populateQueries?.length) {
      const documentSecurity = collection.get("documentSecurity", false);

      let cached: any;

      try {
        cached = await this.cache.get(cacheKey, {
          ttl: Database.TTL,
          tags: [collectionKey, documentKey!],
        });
      } catch (e) {
        this.logger.warn(`Failed to load document '${id}' from cache: ${e}`);
      }

      if (cached) {
        doc = new Doc(cached);

        if (collection.getId() !== Database.METADATA) {
          const readPermissions = [
            ...collection.getRead(),
            ...(documentSecurity ? doc.getRead() : []),
          ];

          const authorization = new Authorization(PermissionEnum.Read);
          if (!authorization.$valid(readPermissions)) {
            return new Doc();
          }
        }

        this.trigger(EventsEnum.DocumentRead, doc);
        return doc;
      }

      doc =
        (await this.adapter.getDocument(
          collection.getId(),
          id,
          processedQuery,
          forUpdate,
        )) || new Doc();

      if (!doc.empty() && collection.getId() !== Database.METADATA) {
        const readPermissions = [
          ...collection.getRead(),
          ...(documentSecurity ? doc.getRead() : []),
        ];

        const authorization = new Authorization(PermissionEnum.Read);
        if (!authorization.$valid(readPermissions)) {
          return new Doc();
        }
      }
    } else {
      const authorization = new Authorization(PermissionEnum.Read);
      if (
        collection.getId() !== Database.METADATA &&
        !authorization.$valid(collection.getRead())
      ) {
        return new Doc();
      }

      const queryWithId = {
        ...processedQuery,
        filters: [Query.equal("$id", [id])],
      };

      const documents = await this.adapter.find(collectionId, queryWithId);
      const processedDocuments = this.processFindResults(
        documents,
        queryWithId,
      );
      doc = processedDocuments[0] || new Doc();
    }

    if (doc.empty()) {
      return doc;
    }

    doc = this.cast(collection, doc);
    doc = await this.decode(processedQuery, doc);

    if (!processedQuery.populateQueries?.length) {
      try {
        await this.cache.set(cacheKey, doc.toObject(), {
          ttl: Database.TTL,
          tags: [collectionKey, documentKey!],
        });
      } catch (e) {
        this.logger.warn(`Failed to save document '${id}' to cache: ${e}`);
      }
    }

    this.trigger(EventsEnum.DocumentRead, doc);
    return doc;
  }

  /**
   * Create a new document.
   */
  public async createDocument<C extends keyof Entities>(
    collectionId: C,
    document: Doc<Entities[C]> | Entities[C],
  ): Promise<Doc<Entities[C]>>;
  public async createDocument<
    D extends Record<string, unknown>,
    C extends string,
  >(
    collectionId: C,
    document: C extends keyof Entities
      ? Doc<Entities[C]> | Entities[C]
      : Doc<D> | D,
  ): Promise<Doc<D>>;
  public async createDocument(
    collectionId: string,
    document: Doc<Partial<IEntity>> | Partial<IEntity>,
  ): Promise<Doc<Partial<IEntity>>> {
    if (
      collectionId !== Database.METADATA &&
      this.adapter.$sharedTables &&
      !this.adapter.$tenantPerDocument &&
      !this.adapter.$tenantId
    ) {
      throw new DatabaseException(
        "Missing tenant. Tenant must be set when table sharing is enabled.",
      );
    }

    if (!this.adapter.$sharedTables && this.adapter.$tenantPerDocument) {
      throw new DatabaseException(
        "Shared tables must be enabled if tenant per document is enabled.",
      );
    }

    const collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );

    if (collection.getId() !== Database.METADATA) {
      const authorization = new Authorization(PermissionEnum.Create);
      if (!authorization.$valid(collection.getCreate())) {
        throw new AuthorizationException(authorization.$description);
      }
    }

    const time = new Date().toISOString();
    let doc: Doc<any> = document instanceof Doc ? document : new Doc(document);

    const createdAt = doc.get("$createdAt");
    const updatedAt = doc.get("$updatedAt");

    doc
      .set("$id", doc.getId() ?? ID.unique())
      .set("$collection", collection.getId())
      .set(
        "$createdAt",
        createdAt === null || createdAt === undefined || !this.preserveDates
          ? time
          : createdAt,
      )
      .set(
        "$updatedAt",
        updatedAt === null || updatedAt === undefined || !this.preserveDates
          ? time
          : updatedAt,
      );

    if (this.adapter.$sharedTables) {
      if (this.adapter.$tenantPerDocument) {
        if (
          collection.getId() !== Database.METADATA &&
          doc.getTenant() === null
        ) {
          throw new DatabaseException(
            "Missing tenant. Tenant must be set when tenant per document is enabled.",
          );
        }
      } else {
        doc.set("$tenant", this.adapter.$tenantId);
      }
    }

    doc = await this.encode(collection, doc);

    if (this.validate) {
      const validator = new Permissions();
      if (!validator.$valid(doc.get("$permissions", []))) {
        throw new DatabaseException(validator.$description);
      }
    }

    const structure = new Structure(collection);
    if (!(await structure.$valid(doc, true))) {
      throw new StructureException(structure.$description);
    }

    const result = await this.withTransaction(async (db) => {
      doc = await this.silent(() => db.createRelationships(collection, doc));
      return db.adapter.createDocument(collection.getId(), doc);
    });

    const castedResult = this.cast(collection, result);
    const decodedResult = await this.decode(
      { collection, populateQueries: [] },
      castedResult,
    );

    this.trigger(EventsEnum.DocumentCreate, decodedResult);

    return decodedResult;
  }

  private async createRelationships(
    collection: Doc<Collection>,
    document: Doc<any>,
  ): Promise<Doc<any>> {
    const relationships = collection
      .get("attributes", [])
      .filter((attr) => attr.get("type") === AttributeEnum.Relationship);

    for (const relationship of relationships) {
      const options = relationship.get("options", {}) as RelationOptions;
      const relatedCollectionId = options.relatedCollection;
      if (!relatedCollectionId) continue;

      const type = options.relationType;
      const side = options.side;
      const value = document.get(relationship.get("key"));
      if (!value) continue;

      // Prevent infinite recursion
      const loopKey = `${collection.getId()}::${document.getId()}::${relationship.getId()}`;
      if (this._relationStack.includes(loopKey)) continue;
      this._relationStack.push(loopKey);

      try {
        if (
          type === RelationEnum.OneToOne ||
          (type === RelationEnum.OneToMany &&
            side === RelationSideEnum.Child) ||
          (type === RelationEnum.ManyToOne && side === RelationSideEnum.Parent)
        ) {
          const relatedDoc = await this.silent(() =>
            this.getDocument(options.relatedCollection, value),
          );

          if (relatedDoc.empty() && !this.checkRelationshipsExist) {
            throw new RelationshipException(
              `Related document '${value}' not found`,
            );
          }

          if (type === RelationEnum.OneToOne) {
            if (options.side === RelationSideEnum.Child && !options.twoWay) {
              throw new DatabaseException(
                `Cannot update OneToOne from child side without twoWay`,
              );
            }

            if (options.twoWay) {
              relatedDoc.set(options.twoWayKey!, document.getId());
              await this.silent(() =>
                this.skipCheckRelationshipsExist(() =>
                  this.updateDocument(
                    options.relatedCollection,
                    value,
                    relatedDoc,
                  ),
                ),
              );
            }
          }
        }

        if (
          type === RelationEnum.ManyToMany ||
          (type === RelationEnum.OneToMany &&
            side === RelationSideEnum.Parent) ||
          (type === RelationEnum.ManyToOne && side === RelationSideEnum.Child)
        ) {
          const { setIds } = this.formatRelationValue(value);
          if (!setIds) continue;
          if (type === RelationEnum.ManyToMany) {
            await this.handleManyToMany(
              collection,
              document,
              relationship,
              options,
              setIds,
            );
          } else {
            for (const childId of setIds) {
              const childDoc = await this.silent(() =>
                this.getDocument(options.relatedCollection, childId),
              );
              if (childDoc.empty() && !this.checkRelationshipsExist) {
                throw new RelationshipException(`Child '${childId}' not found`);
              }
              childDoc.set(options.twoWayKey!, document.getId());
              await this.silent(() =>
                this.skipCheckRelationshipsExist(() =>
                  this.updateDocument(
                    options.relatedCollection,
                    childId,
                    childDoc,
                  ),
                ),
              );
            }
          }
          document.delete(relationship.get("key"));
        }
      } finally {
        this._relationStack.pop();
      }
    }
    return document;
  }

  /**
   * Many-to-Many handling
   */
  private async handleManyToMany(
    collection: Doc<Collection>,
    document: Doc<any>,
    relationship: Doc<Attribute>,
    options: RelationOptions,
    setIds: string[] | null | undefined = undefined,
    connectIds: string[] = [],
    disconnectIds: string[] = [],
  ): Promise<void> {
    // Skip if nothing to do
    if (
      setIds === undefined &&
      connectIds.length === 0 &&
      disconnectIds.length === 0
    ) {
      return;
    }

    const relatedCollection = await this.silent(() =>
      this.getCollection(options.relatedCollection, true),
    );

    const parentColl =
      options.side === RelationSideEnum.Parent ? collection : relatedCollection;
    const childColl =
      options.side === RelationSideEnum.Parent ? relatedCollection : collection;
    const parentAttr =
      options.side === RelationSideEnum.Parent
        ? relationship.getId()
        : options.twoWayKey!;
    const childAttr =
      options.side === RelationSideEnum.Parent
        ? options.twoWayKey!
        : relationship.getId();
    const junctionCollection = this.getJunctionTable(
      parentColl.getSequence(),
      childColl.getSequence(),
      parentAttr,
      childAttr,
    );

    if (setIds !== undefined) {
      await Authorization.skip(() =>
        this.silent(() =>
          this.deleteDocuments(junctionCollection, [
            Query.equal(relationship.getId(), [document.getId()]),
          ]),
        ),
      );
    } else if (disconnectIds.length > 0) {
      await Authorization.skip(() =>
        this.silent(() =>
          this.deleteDocuments(junctionCollection, [
            Query.equal(relationship.getId(), [document.getId()]),
            Query.equal(options.twoWayKey!, disconnectIds),
          ]),
        ),
      );
    }

    const targetIds = setIds !== undefined ? setIds : connectIds;
    const uniqueTargetIds = Array.from(new Set(targetIds)); // de-dupe but keep insertion order

    if (uniqueTargetIds.length > 0) {
      const relatedDocs = await this.silent(() =>
        this.find(options.relatedCollection, (qb) =>
          qb.equal("$id", ...uniqueTargetIds),
        ),
      );

      const foundIds = relatedDocs.map((d) => d.getId());
      const missingIds = uniqueTargetIds.filter((id) => !foundIds.includes(id));

      if (missingIds.length > 0) {
        throw new RelationshipException(
          `Some related documents were not found: ${missingIds.join(", ")}`,
        );
      }

      const linkDocs = uniqueTargetIds.map(
        (relatedId) =>
          new Doc({
            $id: ID.unique(),
            [relationship.getId()]: document.getId(),
            [options.twoWayKey!]: relatedId,
            $permissions: [
              Permission.read(Role.any()),
              Permission.create(Role.any()),
              Permission.delete(Role.any()),
            ],
          }),
      );

      await this.silent(() =>
        this.createDocuments(junctionCollection, linkDocs),
      );
    }
  }

  /**
   * Create multiple documents in a collection.
   */
  public async createDocuments<C extends string & keyof Entities>(
    collectionId: C,
    documents: Doc<Entities[C]>[] | Entities[C][],
  ): Promise<Doc<Entities[C]>[]>;
  public async createDocuments<
    D extends Doc<Record<string, any>>,
    C extends string,
  >(
    collectionId: C,
    documents: C extends keyof Entities
      ? Doc<Entities[C]>[] | Entities[C][]
      : Doc<D>[] | D[],
  ): Promise<D[]>;
  public async createDocuments<D extends Doc<Record<string, any>>>(
    collectionId: string,
    documents: D[],
  ): Promise<Doc[]> {
    if (!documents || documents.length === 0) {
      return [];
    }
    if (collectionId === Database.METADATA) {
      throw new DatabaseException(
        "Cannot create documents in metadata collection",
      );
    }

    const collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );
    if (collection.getId() !== Database.METADATA) {
      const authorization = new Authorization(PermissionEnum.Create);
      if (!authorization.$valid(collection.getCreate())) {
        throw new AuthorizationException(authorization.$description);
      }
    }

    const time = new Date().toISOString();
    const createdDocuments: Doc<any>[] = [];
    for (const document of documents) {
      let doc: Doc<any> =
        document instanceof Doc ? document : new Doc(document);

      const createdAt = doc.get("$createdAt");
      const updatedAt = doc.get("$updatedAt");

      doc
        .set("$id", doc.getId() ?? ID.unique())
        .set("$collection", collection.getId())
        .set(
          "$createdAt",
          createdAt === null || createdAt === undefined || !this.preserveDates
            ? time
            : createdAt,
        )
        .set(
          "$updatedAt",
          updatedAt === null || updatedAt === undefined || !this.preserveDates
            ? time
            : updatedAt,
        );

      if (this.adapter.$sharedTables) {
        if (this.adapter.$tenantPerDocument) {
          if (
            collection.getId() !== Database.METADATA &&
            doc.getTenant() === null
          ) {
            throw new DatabaseException(
              "Missing tenant. Tenant must be set when tenant per document is enabled.",
            );
          }
        } else {
          doc.set("$tenant", this.adapter.$tenantId);
        }
      }

      doc = await this.encode(collection, doc);

      if (this.validate) {
        const validator = new Permissions();
        if (!validator.$valid(doc.get("$permissions", []))) {
          throw new DatabaseException(validator.$description);
        }
      }

      const structure = new Structure(collection);
      if (!(await structure.$valid(doc, true))) {
        throw new StructureException(structure.$description);
      }

      createdDocuments.push(doc);
    }

    const updatedDocuments = await this.withTransaction(async (db) => {
      const resolvedDocuments = await Promise.all(
        createdDocuments.map((doc) => db.createRelationships(collection, doc)),
      );
      return db.adapter.createDocuments(collection.getId(), resolvedDocuments);
    });
    const castedDocuments = updatedDocuments.map((doc) =>
      this.cast(collection, doc),
    );
    const decodedDocuments = await Promise.all(
      castedDocuments.map((doc) =>
        this.decode({ collection, populateQueries: [] }, doc),
      ),
    );

    return decodedDocuments as any[];
  }

  /**
   * Update a document.
   */
  public async updateDocument<C extends string & keyof Entities>(
    collectionId: C,
    id: string,
    document: Entities[C] | Doc<Entities[C]>,
  ): Promise<Doc<Entities[C]>>;
  public async updateDocument<D extends Doc<Record<string, any>>>(
    collectionId: string,
    id: string,
    document: D | Doc<D>,
  ): Promise<D>;
  public async updateDocument(
    collectionId: string,
    id: string,
    document: Doc<Record<string, any>> | Record<string, any>,
  ): Promise<Doc<any>> {
    if (!id) {
      throw new DatabaseException("Must define $id attribute");
    }

    const collection = await this.silent(() =>
      this.getCollection(collectionId),
    );
    const newUpdatedAt = document.updatedAt();
    const updatedDocument = await this.withTransaction(async (db) => {
      const time = new Date().toISOString();
      const old = await this.silent(() =>
        db.getDocument(collection.getId(), id, [], true),
      );

      if (old.empty()) {
        return new Doc();
      }

      let skipPermissionsUpdate = true;

      if (document.getPermissions()) {
        const originalPermissions = old.getPermissions();
        const currentPermissions = document.getPermissions();

        originalPermissions.sort();
        currentPermissions.sort();

        skipPermissionsUpdate =
          JSON.stringify(originalPermissions) ===
          JSON.stringify(currentPermissions);
      }

      const createdAt = document.createdAt();

      const mergedDocument: Record<string, any> = {
        ...old.toObject(),
        ...(document instanceof Doc ? document.toObject() : document),
        $collection: old.get("$collection"),
        $createdAt:
          createdAt === null || !this.preserveDates
            ? old.get("$createdAt")
            : createdAt,
      };

      if (db.adapter.$sharedTables) {
        mergedDocument["$tenant"] = old.get("$tenant");
      }

      const relationships = collection
        .get("attributes", [])
        .filter((attr) => attr.get("type") === AttributeEnum.Relationship);

      let shouldUpdate = false;

      if (collection.getId() !== Database.METADATA) {
        const documentSecurity = collection.get("documentSecurity", false);

        for (const key in mergedDocument) {
          const value = mergedDocument[key];
          const oldValue = old.get(key);

          if (relationships.some((rel) => rel.get("key") === key)) {
            if (value !== undefined) {
              shouldUpdate = true;
            }
          }

          if (value !== oldValue) {
            shouldUpdate = true;
            break;
          }
        }

        const updatePermissions = [
          ...collection.getUpdate(),
          ...(documentSecurity ? old.getUpdate() : []),
        ];

        const readPermissions = [
          ...collection.getRead(),
          ...(documentSecurity ? old.getRead() : []),
        ];

        if (
          shouldUpdate &&
          !new Authorization(PermissionEnum.Update).$valid(updatePermissions)
        ) {
          throw new AuthorizationException("Update not authorized");
        } else if (
          !shouldUpdate &&
          !new Authorization(PermissionEnum.Read).$valid(readPermissions)
        ) {
          throw new AuthorizationException("Read not authorized");
        }
      }

      if (shouldUpdate) {
        mergedDocument["$updatedAt"] =
          newUpdatedAt === null || !this.preserveDates ? time : newUpdatedAt;
      }
      let doc = new Doc(mergedDocument);
      const structureValidator = new Structure(collection);
      if (!structureValidator.$valid(doc)) {
        throw new StructureException(structureValidator.$description);
      }

      const encodedDocument = await db.encode(collection, doc);

      if (relationships.length > 0) {
        doc = await db.updateDocumentRelationships(collection, encodedDocument);
      }
      await db.adapter.updateDocument(
        collection.getId(),
        doc as Doc<IEntity>,
        skipPermissionsUpdate,
      );
      await db.purgeCachedDocument(collection.getId(), encodedDocument);

      return encodedDocument;
    });

    if (updatedDocument.empty()) {
      return updatedDocument;
    }

    const castedDocument = this.cast(collection, updatedDocument);
    const decodedDocument = await this.decode(
      { collection, populateQueries: [] },
      castedDocument,
    );

    this.trigger(EventsEnum.DocumentUpdate, decodedDocument);

    return decodedDocument;
  }

  /**
   * Update relationships of a document.
   */
  private async updateDocumentRelationships(
    collection: Doc<Collection>,
    document: Doc<Record<string, any>>,
  ) {
    const relationships = collection
      .get("attributes", [])
      .filter((attr) => attr.get("type") === AttributeEnum.Relationship);

    for (const relationship of relationships) {
      const options = relationship.get("options", {}) as RelationOptions;
      const relatedCollectionId = options.relatedCollection;
      if (!relatedCollectionId) continue;

      const type = options.relationType;
      const side = options.side;
      const value = document.get(relationship.get("key"), undefined);

      if (value === undefined) continue;

      // Prevent infinite recursion
      const loopKey = `${collection.getId()}::${document.getId()}::${relationship.getId()}`;
      if (this._relationStack.includes(loopKey)) continue;
      this._relationStack.push(loopKey);

      try {
        if (
          type === RelationEnum.OneToOne ||
          (type === RelationEnum.OneToMany &&
            side === RelationSideEnum.Child) ||
          (type === RelationEnum.ManyToOne && side === RelationSideEnum.Parent)
        ) {
          if (value !== null && typeof value !== "string") {
            throw new DatabaseException(
              "Invalid value for relationship: must be a string or",
            );
          }

          const relatedDoc = await this.silent(() =>
            this.getDocument(options.relatedCollection, value),
          );

          if (relatedDoc.empty() && !this.checkRelationshipsExist) {
            throw new RelationshipException(
              `Related document '${value}' not found`,
            );
          }

          if (type === RelationEnum.OneToOne) {
            if (options.side === RelationSideEnum.Child && !options.twoWay) {
              throw new DatabaseException(
                `Cannot update OneToOne from child side without twoWay`,
              );
            }

            if (options.twoWay) {
              // Clear previous relationship
              await this.silent(() =>
                this.skipCheckRelationshipsExist(() =>
                  this.updateDocuments(
                    options.relatedCollection,
                    new Doc({ [options.twoWayKey!]: null }),
                    (qb) => qb.equal(options.twoWayKey!, document.getId()),
                  ),
                ),
              );

              // Set new relationship
              if (value !== null && typeof value === "string") {
                await this.silent(() =>
                  this.skipCheckRelationshipsExist(() =>
                    this.updateDocument(
                      options.relatedCollection,
                      value,
                      new Doc({ [options.twoWayKey!]: document.getId() }),
                    ),
                  ),
                );
              }
            }
          }
        } else if (
          type === RelationEnum.ManyToMany ||
          (type === RelationEnum.OneToMany &&
            side === RelationSideEnum.Parent) ||
          (type === RelationEnum.ManyToOne && side === RelationSideEnum.Child)
        ) {
          const { setIds, connectIds, disconnectIds } =
            this.formatRelationValue(value);
          // Remove the relationship attribute from the document to prevent errors during the main document update
          document.delete(relationship.get("key"));

          if (
            setIds === undefined &&
            connectIds.length === 0 &&
            disconnectIds.length === 0
          ) {
            continue;
          }

          if (type === RelationEnum.ManyToMany) {
            await this.handleManyToMany(
              collection,
              document,
              relationship,
              options,
              setIds,
              connectIds,
              disconnectIds,
            );
          } else {
            // If SET mode
            if (setIds !== undefined) {
              // Clear all current children
              await this.silent(() =>
                this.skipCheckRelationshipsExist(() =>
                  this.updateDocuments(
                    options.relatedCollection,
                    new Doc({ [options.twoWayKey!]: null }),
                    (qb) => qb.equal(options.twoWayKey!, document.getId()),
                  ),
                ),
              );

              //  If new set is not empty, set new children
              if (setIds && setIds.length > 0) {
                await this.silent(() =>
                  this.skipCheckRelationshipsExist(() =>
                    this.updateDocuments(
                      options.relatedCollection,
                      new Doc({ [options.twoWayKey!]: document.getId() }),
                      [Query.equal("$id", setIds)],
                    ),
                  ),
                );
              }
            }
            // Else CONNECT/DISCONNECT mode
            else {
              // Remove overlaps
              const connectSet = new Set(connectIds);
              const disconnectSet = new Set(disconnectIds);
              for (const id of connectSet) disconnectSet.delete(id);

              // Disconnect
              if (disconnectSet.size > 0) {
                await this.silent(() =>
                  this.skipCheckRelationshipsExist(() =>
                    this.updateDocuments(
                      options.relatedCollection,
                      new Doc({ [options.twoWayKey!]: null }),
                      [Query.equal("$id", Array.from(disconnectSet))],
                    ),
                  ),
                );
              }

              // Connect
              if (connectSet.size > 0) {
                await this.silent(() =>
                  this.skipCheckRelationshipsExist(() =>
                    this.updateDocuments(
                      options.relatedCollection,
                      new Doc({ [options.twoWayKey!]: document.getId() }),
                      [Query.equal("$id", Array.from(connectSet))],
                    ),
                  ),
                );
              }
            }
          }
        }
      } finally {
        this._relationStack.pop();
      }
    }
    return document;
  }

  /**
   * Update multiple documents in a collection.
   */
  public async updateDocuments<C extends string & keyof Entities>(
    collectionId: C,
    updates: Doc<Entities[C]>,
    query?: Query[] | ((qb: QueryBuilder) => QueryBuilder),
    batchSize?: number,
    onNext?: (doc: Doc<Entities[C]>) => void | Promise<void>,
    onError?: (error: Error) => void | Promise<void>,
  ): Promise<number>;
  public async updateDocuments<
    D extends Doc<Record<string, any>>,
    C extends string,
  >(
    collectionId: C,
    updates: C extends keyof Entities ? Doc<Entities[C]> : D,
    query?: Query[] | ((qb: QueryBuilder) => QueryBuilder),
    batchSize?: number,
    onNext?: (doc: D) => void | Promise<void>,
    onError?: (error: Error) => void | Promise<void>,
  ): Promise<number>;
  public async updateDocuments(
    collectionId: string,
    updates: Doc<Partial<IEntity> & Record<string, any>>,
    query: Query[] | ((qb: QueryBuilder) => QueryBuilder) = [],
    batchSize: number = Database.DEFAULT_BATCH_SIZE,
    onNext?: (doc: Doc<any>) => void | Promise<void>,
    onError?: (error: Error) => void | Promise<void>,
  ): Promise<number> {
    if (updates.empty()) {
      return 0;
    }
    let queries: Query[];
    if (typeof query === "function") {
      queries = query(new QueryBuilder()).build();
    } else queries = query;

    batchSize = Math.min(1000, Math.max(1, batchSize));
    const collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );

    const documentSecurity = collection.get("documentSecurity", false);
    const authorization = new Authorization(PermissionEnum.Update);
    const skipAuth = authorization.$valid(collection.getUpdate());

    if (
      !skipAuth &&
      !documentSecurity &&
      collection.getId() !== Database.METADATA
    ) {
      throw new AuthorizationException(authorization.$description);
    }

    const attributes = collection.get("attributes", []);
    const indexes = collection.get("indexes", []);

    if (this.validate) {
      const validator = new Documents(attributes, indexes, this.maxQueryValues);

      if (!validator.$valid(queries)) {
        throw new QueryException(validator.$description);
      }
    }

    const grouped = Query.groupByType(queries);
    let { limit, cursor } = grouped;

    if (cursor && cursor.getCollection() !== collection.getId()) {
      throw new DatabaseException(
        "Cursor document must be from the same Collection.",
      );
    }

    // Prepare updates document
    const updatesClone = updates.clone();
    updatesClone.delete("$id");
    updatesClone.delete("$tenant");

    if (updatesClone.createdAt() === null || !this.preserveDates) {
      updatesClone.delete("$createdAt");
    } else {
      updatesClone.set("$createdAt", updatesClone.createdAt());
    }

    if (this.adapter.$sharedTables) {
      updatesClone.set("$tenant", this.adapter.$tenantId);
    }

    const updatedAt = updatesClone.updatedAt();
    const time = new Date().toISOString();
    updatesClone.set(
      "$updatedAt",
      updatedAt === null || !this.preserveDates ? time : updatedAt,
    );

    const encodedUpdates = await this.encode(collection, updatesClone);

    // Validate structure
    const validator = new Structure(collection);
    if (!validator.$valid(encodedUpdates, false)) {
      throw new StructureException(validator.$description);
    }

    const originalLimit = limit;
    let last = cursor as Doc<any>;
    let modified = 0;

    while (true) {
      let currentBatchSize = batchSize;
      if (originalLimit !== null && originalLimit < batchSize) {
        currentBatchSize = originalLimit;
      }

      const batchQueries = [Query.limit(currentBatchSize)];
      if (last) {
        batchQueries.push(Query.cursorAfter(last));
      }

      const batch = await this.silent(() =>
        this.find(
          collection.getId(),
          [...batchQueries, ...queries],
          PermissionEnum.Update,
        ),
      );

      if (batch.length === 0) {
        break;
      }

      const currentPermissions = encodedUpdates.getPermissions();
      currentPermissions.sort();

      await this.withTransaction(async (db) => {
        const processedBatch: Doc<any>[] = [];

        for (let index = 0; index < batch.length; index++) {
          const document = batch[index]!;
          let skipPermissionsUpdate = true;

          if (encodedUpdates.has("$permissions")) {
            if (!document.has("$permissions")) {
              throw new QueryException("Permission document missing in select");
            }

            const originalPermissions = document.getPermissions();
            originalPermissions.sort();

            skipPermissionsUpdate =
              JSON.stringify(originalPermissions) ===
              JSON.stringify(currentPermissions);
          }

          document.set("$skipPermissionsUpdate", skipPermissionsUpdate);
          const newDocument = await this.silent(() =>
            db.updateDocumentRelationships(collection, document),
          );

          const merged = new Doc({
            ...newDocument.toObject(),
            ...encodedUpdates.toObject(),
          });

          // Check if document was updated after the request timestamp
          const oldUpdatedAt = new Date(document.updatedAt()!);
          if (this.timestamp && oldUpdatedAt > this.timestamp) {
            throw new ConflictException(
              "Document was updated after the request timestamp",
            );
          }

          const encodedDocument = await db.encode(collection, merged);
          processedBatch.push(encodedDocument);
        }

        await db.adapter.updateDocuments(
          collection.getId(),
          encodedUpdates,
          processedBatch,
        );
      });

      for (const doc of batch) {
        doc.delete("$skipPermissionsUpdate");

        await this.purgeCachedDocument(collection.getId(), doc.getId());
        const castedDoc = this.cast(collection, doc);
        const decodedDoc = await this.decode(
          { collection, populateQueries: [] },
          castedDoc,
        );

        try {
          if (onNext) {
            const result = onNext(decodedDoc);
            if (result instanceof Promise) {
              await result;
            }
          }
        } catch (error) {
          if (onError) {
            const errorResult = onError(error as Error);
            if (errorResult instanceof Promise) {
              await errorResult;
            }
          } else {
            throw error;
          }
        }
        modified++;
      }

      if (batch.length < currentBatchSize) {
        break;
      }

      last = batch[batch.length - 1]!;
    }

    this.trigger(
      EventsEnum.DocumentsUpdate,
      new Doc({
        $collection: collection.getId(),
        modified: modified,
      }),
    );

    return modified;
  }

  /**
   * Delete document by ID.
   */
  public async deleteDocument<C extends string & keyof Entities>(
    collectionId: C,
    id: string,
  ): Promise<boolean>;
  public async deleteDocument<C extends string>(
    collectionId: C,
    id: string,
  ): Promise<boolean>;
  public async deleteDocument(
    collectionId: string,
    id: string,
  ): Promise<boolean> {
    const collection = await this.silent(() =>
      this.getCollection(collectionId),
    );

    let document!: Doc;
    const deleted = await this.withTransaction(async (db) => {
      document = await Authorization.skip(() =>
        this.silent(() => db.getDocument(collection.getId(), id, [], true)),
      );

      if (document.empty()) {
        return false;
      }

      const validator = new Authorization(PermissionEnum.Delete);

      if (collection.getId() !== Database.METADATA) {
        const documentSecurity = collection.get("documentSecurity", true);
        if (
          !validator.$valid([
            ...collection.getDelete(),
            ...(documentSecurity ? document.getDelete() : []),
          ])
        ) {
          throw new AuthorizationException(validator.$description);
        }
      }

      // Check if document was updated after the request timestamp
      const oldUpdatedAt = new Date(document.updatedAt()!);
      if (this.timestamp && oldUpdatedAt > this.timestamp) {
        throw new ConflictException(
          "Document was updated after the request timestamp",
        );
      }

      await this.silent(() =>
        db.deleteDocumentRelationships(collection, document),
      );
      const result = await db.adapter.deleteDocument(
        collection.getId(),
        document,
      );

      await db.purgeCachedDocument(collection.getId(), id);

      return result;
    });

    this.trigger(
      EventsEnum.DocumentDelete,
      deleted ? document : new Doc({ $id: id }),
    );

    return deleted;
  }

  /**
   * Delete multiple documents in a collection.
   */
  public async deleteDocuments<C extends string & keyof Entities>(
    collectionId: C,
    query?: Query[] | ((qb: QueryBuilder<C>) => QueryBuilder<C>),
  ): Promise<string[]>;
  public async deleteDocuments(
    collectionId: string,
    query?: Query[] | ((qb: QueryBuilder) => QueryBuilder),
  ): Promise<string[]>;
  public async deleteDocuments(
    collectionId: string,
    query?: Query[] | ((qb: QueryBuilder) => QueryBuilder),
  ): Promise<string[]> {
    const collection = await this.silent(() =>
      this.getCollection(collectionId),
    );
    let queries: Query[];
    if (typeof query === "function") {
      queries = query(new QueryBuilder()).build();
    } else queries = query ?? [];

    const deletedIds = await this.withTransaction(async (db) => {
      const processedQueries = await db.processQueries(queries, collection, {
        forPermission: PermissionEnum.Delete,
      });
      const result = await db.adapter.deleteDocuments(
        collection.getId(),
        processedQueries,
      );
      for (const id of result) {
        await db.purgeCachedDocument(collection.getId(), id);
        await db.silent(() =>
          db.deleteDocumentRelationships(
            collection,
            new Doc({
              $id: id,
              $collection: collection.getId(),
            }),
          ),
        );
      }
      return result;
    });

    return deletedIds;
  }

  /**
   * Delete multiple documents in a collection with batch processing.
   *
   */
  public async deleteDocumentsBatch<C extends string & keyof Entities>(
    collectionId: C,
    query?: Query[] | ((qb: QueryBuilder) => QueryBuilder),
    batchSize?: number,
    onNext?: (
      doc: Doc<Entities[C]>,
      old: Doc<Entities[C]>,
    ) => void | Promise<void>,
    onError?: (error: Error) => void | Promise<void>,
  ): Promise<number>;
  public async deleteDocumentsBatch<D extends IEntity = IEntity>(
    collectionId: string,
    query?: Query[] | ((qb: QueryBuilder) => QueryBuilder),
    batchSize?: number,
    onNext?: (doc: Doc<D>, old: Doc<D>) => void | Promise<void>,
    onError?: (error: Error) => void | Promise<void>,
  ): Promise<number>;
  public async deleteDocumentsBatch(
    collectionId: string,
    query?: Query[] | ((qb: QueryBuilder) => QueryBuilder),
    batchSize: number = Database.DELETE_BATCH_SIZE,
    onNext?: (doc: Doc<any>, old: Doc<any>) => void | Promise<void>,
    onError?: (error: Error) => void | Promise<void>,
  ): Promise<number> {
    if (this.adapter.$sharedTables && !this.adapter.$tenantId) {
      throw new DatabaseException(
        "Missing tenant. Tenant must be set when table sharing is enabled.",
      );
    }

    batchSize = Math.min(Database.DELETE_BATCH_SIZE, Math.max(1, batchSize));
    const collection = await this.silent(() =>
      this.getCollection(collectionId),
    );

    if (collection.empty()) {
      throw new NotFoundException("Collection not found");
    }

    const documentSecurity = collection.get("documentSecurity", false);
    const authorization = new Authorization(PermissionEnum.Delete);
    const skipAuth = authorization.$valid(collection.getDelete());

    if (
      !skipAuth &&
      !documentSecurity &&
      collection.getId() !== Database.METADATA
    ) {
      throw new AuthorizationException(authorization.$description);
    }

    let queries: Query[];
    if (typeof query === "function") {
      queries = query(new QueryBuilder()).build();
    } else {
      queries = query ?? [];
    }

    const attributes = collection.get("attributes", []);
    const indexes = collection.get("indexes", []);

    if (this.validate) {
      const validator = new Documents(attributes, indexes, this.maxQueryValues);

      if (!validator.$valid(queries)) {
        throw new QueryException(validator.$description);
      }
    }

    const grouped = Query.groupByType(queries);
    let { limit, cursor } = grouped;

    if (cursor && cursor.getCollection() !== collection.getId()) {
      throw new DatabaseException(
        "Cursor document must be from the same Collection.",
      );
    }

    const originalLimit = limit;
    let last = cursor as Doc<any>;
    let modified = 0;

    while (true) {
      let currentBatchSize = batchSize;
      if (limit && limit < batchSize && limit > 0) {
        currentBatchSize = limit;
      } else if (limit) {
        limit -= batchSize;
      }

      const batchQueries = [Query.limit(currentBatchSize)];
      if (last) {
        batchQueries.push(Query.cursorAfter(last));
      }

      const batch = await this.silent(() =>
        this.find(
          collection.getId(),
          [...batchQueries, ...queries],
          PermissionEnum.Delete,
        ),
      );

      if (batch.length === 0) {
        break;
      }

      const old = batch.map((doc) => doc.clone());
      const sequences: number[] = [];
      const permissionIds: string[] = [];

      await this.withTransaction(async (db) => {
        for (const document of batch) {
          sequences.push(document.getSequence());
          if (document.getPermissions().length > 0) {
            permissionIds.push(document.getId());
          }

          if (this.resolveRelationships) {
            await this.silent(() =>
              db.deleteDocumentRelationships(collection, document),
            );
          }

          // Check if document was updated after the request timestamp
          const oldUpdatedAt = new Date(document.updatedAt()!);
          if (this.timestamp && oldUpdatedAt > this.timestamp) {
            throw new ConflictException(
              "Document was updated after the request timestamp",
            );
          }
        }

        await db.adapter.deleteDocumentsBySequences(
          collection.getId(),
          sequences,
          permissionIds,
        );
      });

      for (let index = 0; index < batch.length; index++) {
        const document = batch[index]!;
        const oldDocument = old[index]!;

        if (this.adapter.$sharedTables && this.adapter.$tenantPerDocument) {
          await this.withTenant(document.getTenant(), () =>
            this.purgeCachedDocument(collection.getId(), document.getId()),
          );
        } else {
          await this.purgeCachedDocument(collection.getId(), document.getId());
        }

        try {
          if (onNext) {
            const result = onNext(document, oldDocument);
            if (result instanceof Promise) {
              await result;
            }
          }
        } catch (error) {
          if (onError) {
            const errorResult = onError(error as Error);
            if (errorResult instanceof Promise) {
              await errorResult;
            }
          } else {
            throw error;
          }
        }
        modified++;
      }

      if (batch.length < currentBatchSize) {
        break;
      } else if (originalLimit && modified >= originalLimit) {
        break;
      }

      last = batch[batch.length - 1]!;
    }

    this.trigger(
      EventsEnum.DocumentsDelete,
      new Doc({
        $collection: collection.getId(),
        modified: modified,
      }),
    );

    return modified;
  }

  /**
   * Delete all relationships of a document.
   */
  private async deleteDocumentRelationships(
    collection: Doc<Collection>,
    document: Doc<Record<string, any>>,
  ) {
    const relationships = collection
      .get("attributes", [])
      .filter((attr) => attr.get("type") === AttributeEnum.Relationship);

    for (const relationship of relationships) {
      const options = relationship.get("options", {}) as RelationOptions;
      const relatedCollectionId = options.relatedCollection;
      if (!relatedCollectionId) continue;

      const loopKey = `${collection.getId()}::${document.getId()}::${relationship.getId()}`;
      if (this._relationStack.includes(loopKey)) continue;
      this._relationStack.push(loopKey);

      try {
        await this.handleOnDelete(collection, document, relationship, options);
      } finally {
        this._relationStack.pop();
      }
    }
  }

  /**
   * Handle deletion of related documents based on the relationship options.
   * This method is called when a document is deleted and handles the cascading effects
   * according to the `onDelete` option specified in the relationship.
   */
  private async handleOnDelete(
    collection: Doc<Collection>,
    document: Doc<Record<string, any>>,
    relationship: Doc<Attribute>,
    options: RelationOptions,
  ): Promise<void> {
    const type = options.relationType;
    const side = options.side;
    const onDelete = options.onDelete;

    let targetCollectionId: string | undefined;
    let targetField: string | null = null;
    let isManyToMany = false;

    // Identify relation mapping
    if (type === RelationEnum.ManyToMany) {
      isManyToMany = true;
    } else if (type === RelationEnum.OneToOne) {
      if (side === RelationSideEnum.Parent) {
        targetCollectionId = options.relatedCollection;
        targetField = options.twoWayKey!;
      } else {
        targetCollectionId = collection.getId();
        targetField = relationship.getId();
      }
    } else if (type === RelationEnum.OneToMany) {
      if (side === RelationSideEnum.Parent) {
        targetCollectionId = options.relatedCollection;
        targetField = options.twoWayKey!;
      } else {
        targetCollectionId = collection.getId();
        targetField = relationship.getId();
      }
    } else if (type === RelationEnum.ManyToOne) {
      if (side === RelationSideEnum.Parent) {
        targetCollectionId = collection.getId();
        targetField = relationship.getId();
      } else {
        targetCollectionId = options.relatedCollection;
        targetField = options.twoWayKey!;
      }
    }

    if (isManyToMany) {
      const relatedCollection = await this.getCollection(
        options.relatedCollection,
        true,
      );
      const parentColl =
        side === RelationSideEnum.Parent ? collection : relatedCollection;
      const childColl =
        side === RelationSideEnum.Parent ? relatedCollection : collection;
      const parentAttr =
        side === RelationSideEnum.Parent
          ? relationship.getId()
          : options.twoWayKey!;
      const childAttr =
        side === RelationSideEnum.Parent
          ? options.twoWayKey!
          : relationship.getId();
      const junctionCollection = this.getJunctionTable(
        parentColl.getSequence(),
        childColl.getSequence(),
        parentAttr,
        childAttr,
      );

      if (onDelete === OnDelete.Restrict) {
        const count = await Authorization.skip(() =>
          this.count(
            junctionCollection,
            [Query.equal(parentAttr, [document.getId()])],
            1,
          ),
        );
        if (count > 0) {
          throw new RelationshipException(
            `Cannot delete: related entries exist in "${relatedCollection.getId()}".`,
          );
        }
      } else if (onDelete === OnDelete.SetNull) {
        await Authorization.skip(() =>
          this.deleteDocuments(junctionCollection, [
            Query.equal(parentAttr, [document.getId()]),
          ]),
        );
      } else if (onDelete === OnDelete.Cascade) {
        const relatedIds = (
          await this.find(junctionCollection, (qb) =>
            qb.equal(parentAttr, document.getId()),
          )
        ).map((doc) => doc.get(childAttr));

        await this.deleteDocuments(junctionCollection, [
          Query.equal(parentAttr, [document.getId()]),
        ]);

        relatedIds.length &&
          (await this.deleteDocuments(relatedCollection.getId(), (qb) =>
            qb.equal("$id", ...relatedIds),
          ));
      }
      return;
    }

    // Non-ManyToMany
    if (!targetCollectionId || !targetField) return;

    if (onDelete === OnDelete.Restrict) {
      const count = await Authorization.skip(() =>
        this.count(
          targetCollectionId,
          [Query.equal(targetField, [document.getId()])],
          1,
        ),
      );
      if (count > 0) {
        throw new RelationshipException(
          `Cannot delete: related entries exist in "${targetCollectionId}".`,
        );
      }
    } else if (onDelete === OnDelete.SetNull) {
      await Authorization.skip(() =>
        this.updateDocuments(
          targetCollectionId,
          new Doc({ [targetField]: null }),
          [Query.equal(targetField, [document.getId()])],
        ),
      );
    } else if (onDelete === OnDelete.Cascade) {
      await this.deleteDocuments(targetCollectionId, (qb) =>
        qb.equal(targetField, document.getId()),
      );
    }
  }

  /**
   * Create or update documents in a collection.
   */
  public async createOrUpdateDocuments<C extends string & keyof Entities>(
    collectionId: C,
    documents: Doc<Entities[C]>[],
    batchSize?: number,
    onNext?: (doc: Doc<Entities[C]>) => void | Promise<void>,
  ): Promise<number>;
  public async createOrUpdateDocuments<
    D extends Doc<Record<string, any>>,
    C extends string,
  >(
    collectionId: C,
    documents: C extends keyof Entities ? Doc<Entities[C]>[] : D[],
    batchSize?: number,
    onNext?: (doc: D) => void | Promise<void>,
  ): Promise<number>;
  public async createOrUpdateDocuments(
    collectionId: string,
    documents: Doc<Record<string, any>>[],
    batchSize: number = Database.DEFAULT_BATCH_SIZE,
    onNext?: (doc: Doc<any>) => void | Promise<void>,
  ): Promise<number> {
    return this.createOrUpdateDocumentsWithIncrease(
      collectionId,
      "",
      documents,
      batchSize,
      onNext,
    );
  }

  /**
   * Create or update documents, increasing the value of the given attribute by the value in each document.
   */
  public async createOrUpdateDocumentsWithIncrease<
    C extends string & keyof Entities,
  >(
    collectionId: C,
    attribute: keyof Entities[C] & string,
    documents: Doc<Entities[C]>[],
    batchSize?: number,
    onNext?: (doc: Doc<Entities[C]>) => void | Promise<void>,
  ): Promise<number>;
  public async createOrUpdateDocumentsWithIncrease<
    D extends Doc<Record<string, any>>,
    C extends string,
  >(
    collectionId: C,
    attribute: string,
    documents: C extends keyof Entities ? Doc<Entities[C]>[] : D[],
    batchSize?: number,
    onNext?: (doc: D) => void | Promise<void>,
  ): Promise<number>;
  public async createOrUpdateDocumentsWithIncrease(
    collectionId: string,
    attribute: string,
    documents: Doc<Record<string, any>>[],
    batchSize: number = 1000,
    onNext?: (doc: Doc<any>) => void | Promise<void>,
  ): Promise<number> {
    if (!documents || documents.length === 0) {
      return 0;
    }

    batchSize = Math.min(1000, Math.max(1, batchSize));
    const collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );
    const documentSecurity = collection.get("documentSecurity", false);
    const collectionAttributes = collection.get("attributes", []);
    const time = new Date().toISOString();
    let created = 0;
    let updated = 0;
    const seenIds: string[] = [];

    const processedDocuments: Array<{
      old: Doc<any>;
      new: Doc<any>;
    }> = [];

    for (let i = 0; i < documents.length; i++) {
      const document = documents[i]!;

      let old: Doc<any>;
      if (this.adapter.$sharedTables && this.adapter.$tenantPerDocument) {
        old = await Authorization.skip(() =>
          this.withTenant(document.getTenant(), () =>
            this.silent(() =>
              this.getDocument(collection.getId(), document.getId()),
            ),
          ),
        );
      } else {
        old = await Authorization.skip(() =>
          this.silent(() =>
            this.getDocument(collection.getId(), document.getId()),
          ),
        );
      }

      let skipPermissionsUpdate = true;

      if (document.has("$permissions")) {
        const originalPermissions = old.getPermissions();
        const currentPermissions = document.getPermissions();

        originalPermissions.sort();
        currentPermissions.sort();

        skipPermissionsUpdate =
          JSON.stringify(originalPermissions) ===
          JSON.stringify(currentPermissions);
      }

      if (
        !attribute &&
        skipPermissionsUpdate &&
        JSON.stringify(old.toObject([], ["$permissions"])) ===
          JSON.stringify(document.toObject([], ["$permissions"]))
      ) {
        // If not updating a single attribute and the
        // document is the same as the old one, skip it
        continue;
      }

      // Check permissions
      const validator = new Authorization(
        old.empty() ? PermissionEnum.Create : PermissionEnum.Update,
      );

      if (old.empty()) {
        if (!validator.$valid(collection.getCreate())) {
          throw new AuthorizationException(validator.$description);
        }
      } else if (
        !validator.$valid([
          ...collection.getUpdate(),
          ...(documentSecurity ? old.getUpdate() : []),
        ])
      ) {
        throw new AuthorizationException(validator.$description);
      }

      const updatedAt = document.updatedAt();
      const createdAt = document.createdAt();

      document
        .set("$id", document.getId() || ID.unique())
        .set("$collection", collection.getId())
        .set(
          "$updatedAt",
          updatedAt === null || !this.preserveDates ? time : updatedAt,
        )
        .delete("$sequence");

      if (createdAt === null || !this.preserveDates) {
        document.set("$createdAt", old.empty() ? time : old.createdAt());
      } else {
        document.set("$createdAt", createdAt);
      }

      // Force matching optional parameter sets
      for (const attr of collectionAttributes) {
        if (!attr.get("required") && !document.has(attr.get("$id"))) {
          document.set(
            attr.get("$id"),
            old.get(attr.get("$id"), attr.get("default", null)),
          );
        }
      }

      if (skipPermissionsUpdate) {
        document.set("$permissions", old.getPermissions());
      }

      if (this.adapter.$sharedTables) {
        if (this.adapter.$tenantPerDocument) {
          if (document.getTenant() === null) {
            throw new DatabaseException(
              "Missing tenant. Tenant must be set when tenant per document is enabled.",
            );
          }
          if (!old.empty() && old.getTenant() !== document.getTenant()) {
            throw new DatabaseException("Tenant cannot be changed.");
          }
        } else {
          document.set("$tenant", this.adapter.$tenantId);
        }
      }

      const encodedDocument = await this.encode(collection, document);
      const structureValidator = new Structure(collection);
      if (!(await structureValidator.$valid(encodedDocument))) {
        throw new StructureException(structureValidator.$description);
      }

      if (!old.empty()) {
        // Check if document was updated after the request timestamp
        const oldUpdatedAt = new Date(old.updatedAt()!);
        if (this.timestamp && oldUpdatedAt > this.timestamp) {
          throw new ConflictException(
            "Document was updated after the request timestamp",
          );
        }
      }

      if (this.resolveRelationships) {
        await this.silent(() =>
          this.createRelationships(collection, encodedDocument),
        );
      }

      seenIds.push(encodedDocument.getId());
      processedDocuments.push({
        old,
        new: encodedDocument,
      });
    }

    // Required because *some* DBs will allow duplicate IDs for upsert
    if (seenIds.length !== new Set(seenIds).size) {
      throw new DuplicateException(
        "Duplicate document IDs found in the input array.",
      );
    }

    // Process in batches
    const chunks = [];
    for (let i = 0; i < processedDocuments.length; i += batchSize) {
      chunks.push(processedDocuments.slice(i, i + batchSize));
    }

    for (const chunk of chunks) {
      const batch = await this.withTransaction((db) =>
        Authorization.skip(() =>
          db.adapter.createOrUpdateDocuments(
            collection.getId(),
            attribute,
            chunk,
          ),
        ),
      );

      for (const change of chunk) {
        if (change.old.empty()) {
          created++;
        } else {
          updated++;
        }
      }

      for (const doc of batch) {
        let processedDoc = doc;
        processedDoc = this.cast(collection, processedDoc);
        processedDoc = await this.decode(
          { collection, populateQueries: [] },
          processedDoc,
        );

        if (this.adapter.$sharedTables && this.adapter.$tenantPerDocument) {
          await this.withTenant(processedDoc.getTenant(), () =>
            this.purgeCachedDocument(collection.getId(), processedDoc.getId()),
          );
        } else {
          await this.purgeCachedDocument(
            collection.getId(),
            processedDoc.getId(),
          );
        }

        if (onNext) {
          const result = onNext(processedDoc);
          if (result instanceof Promise) {
            await result;
          }
        }
      }
    }

    this.trigger(
      EventsEnum.DocumentsUpsert,
      new Doc({
        $collection: collection.getId(),
        created: created,
        updated: updated,
      }),
    );

    return created + updated;
  }

  /**
   * Increase a numeric attribute value in a document.
   */
  public async increaseDocumentAttribute<C extends string & keyof Entities>(
    collectionId: C,
    id: string,
    attribute: keyof Entities[C] & string,
    value?: number,
    max?: number,
  ): Promise<Doc<Entities[C]>>;
  public async increaseDocumentAttribute<C extends string>(
    collectionId: C,
    id: string,
    attribute: string,
    value?: number,
    max?: number,
  ): Promise<Doc<any>>;
  public async increaseDocumentAttribute(
    collectionId: string,
    id: string,
    attribute: string,
    value: number = 1,
    max?: number,
  ): Promise<Doc<any>> {
    if (value <= 0) {
      throw new DatabaseException("Value must be numeric and greater than 0");
    }

    const collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );

    const attr = collection
      .get("attributes", [])
      .find(
        (a: Doc<Attribute>) =>
          a.get("$id") === attribute || a.get("key") === attribute,
      );

    if (!attr) {
      throw new NotFoundException("Attribute not found");
    }

    const whiteList = [AttributeEnum.Integer, AttributeEnum.Float];

    if (!whiteList.includes(attr.get("type")) || attr.get("array")) {
      throw new DatabaseException(
        "Attribute must be an integer or float and can not be an array.",
      );
    }

    const document = await this.withTransaction(async (db) => {
      const doc = await Authorization.skip(() =>
        db.silent(() => db.getDocument(collection.getId(), id, [], true)),
      );

      if (doc.empty()) {
        throw new NotFoundException("Document not found");
      }

      const validator = new Authorization(PermissionEnum.Update);

      if (collection.getId() !== Database.METADATA) {
        const documentSecurity = collection.get("documentSecurity", false);
        if (
          !validator.$valid([
            ...collection.getUpdate(),
            ...(documentSecurity ? doc.getUpdate() : []),
          ])
        ) {
          throw new AuthorizationException(validator.$description);
        }
      }

      const currentValue = doc.get(attribute);
      if (max !== undefined && currentValue + value > max) {
        throw new LimitException(
          `Attribute value exceeds maximum limit: ${max}`,
        );
      }

      const time = new Date().toISOString();
      const updatedAt = doc.get("$updatedAt");
      const finalUpdatedAt =
        !updatedAt || !this.preserveDates ? time : updatedAt;
      const maxValue = max !== undefined ? max - value : undefined;

      await db.adapter.increaseDocumentAttribute({
        collection: collection.getId(),
        id,
        attribute,
        value,
        updatedAt: finalUpdatedAt as Date,
        max: maxValue,
      });

      return doc.set(attribute, currentValue + value);
    });

    await this.purgeCachedDocument(collection.getId(), id);

    this.trigger(EventsEnum.DocumentIncrease, document, value);

    return document;
  }

  /**
   * Decrease a numeric attribute value in a document.
   */
  public async decreaseDocumentAttribute<C extends string & keyof Entities>(
    collectionId: C,
    id: string,
    attribute: keyof Entities[C] & string,
    value?: number,
    min?: number,
  ): Promise<Doc<Entities[C]>>;
  public async decreaseDocumentAttribute<C extends string>(
    collectionId: C,
    id: string,
    attribute: string,
    value?: number,
    min?: number,
  ): Promise<Doc<any>>;
  public async decreaseDocumentAttribute(
    collectionId: string,
    id: string,
    attribute: string,
    value: number = 1,
    min?: number,
  ): Promise<Doc<any>> {
    if (value <= 0) {
      throw new DatabaseException("Value must be numeric and greater than 0");
    }

    const collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );

    const attr = collection
      .get("attributes", [])
      .find(
        (a: Doc<Attribute>) =>
          a.get("$id") === attribute || a.get("key") === attribute,
      );

    if (!attr) {
      throw new NotFoundException("Attribute not found");
    }

    const whiteList = [AttributeEnum.Integer, AttributeEnum.Float];

    if (!whiteList.includes(attr.get("type")) || attr.get("array")) {
      throw new DatabaseException(
        "Attribute must be an integer or float and can not be an array.",
      );
    }

    const document = await this.withTransaction(async (db) => {
      const doc = await Authorization.skip(() =>
        db.silent(() => db.getDocument(collection.getId(), id, [], true)),
      );

      if (doc.empty()) {
        throw new NotFoundException("Document not found");
      }

      const validator = new Authorization(PermissionEnum.Update);

      if (collection.getId() !== Database.METADATA) {
        const documentSecurity = collection.get("documentSecurity", false);
        if (
          !validator.$valid([
            ...collection.getUpdate(),
            ...(documentSecurity ? doc.getUpdate() : []),
          ])
        ) {
          throw new AuthorizationException(validator.$description);
        }
      }

      const currentValue = doc.get(attribute);
      if (min !== undefined && currentValue - value < min) {
        throw new LimitException(
          `Attribute value exceeds minimum limit: ${min}`,
        );
      }

      const time = new Date().toISOString();
      const updatedAt = doc.get("$updatedAt");
      const finalUpdatedAt = !updatedAt || !db.preserveDates ? time : updatedAt;
      const minValue = min !== undefined ? min + value : undefined;

      await db.adapter.increaseDocumentAttribute({
        collection: collection.getId(),
        id,
        attribute,
        value: value * -1,
        updatedAt: finalUpdatedAt as Date,
        min: minValue,
      });

      return doc.set(attribute, currentValue - value);
    });

    await this.purgeCachedDocument(collection.getId(), id);

    this.trigger(EventsEnum.DocumentDecrease, document, value);

    return document;
  }

  /**
   * find documents.
   */
  public find<C extends string & keyof Entities>(
    collectionId: C,
    query?: ((builder: QueryBuilder<C>) => QueryBuilder<C>) | Query[],
    forPermission?: PermissionEnum,
  ): Promise<Doc<Entities[C]>[]>;
  public find<C extends string>(
    collectionId: C,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    forPermission?: PermissionEnum,
  ): Promise<Doc<Partial<IEntity> & Record<string, any>>[]>;
  public find<D extends Record<string, any>>(
    collectionId: string,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    forPermission?: PermissionEnum,
  ): Promise<Doc<Partial<IEntity> & D>[]>;
  public async find(
    collectionId: string,
    query: ((builder: QueryBuilder) => QueryBuilder) | Query[] = [],
    forPermission: PermissionEnum = PermissionEnum.Read,
  ): Promise<Doc<any>[]> {
    if (!collectionId) {
      throw new NotFoundException(`Collection '${collectionId}' not found.`);
    }

    const collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );

    const queries: Query[] =
      typeof query === "function" ? query(new QueryBuilder()).build() : query;

    const processedQueries = await this.processQueries(queries, collection, {
      forPermission,
    });

    if (!processedQueries.authorized) {
      return [];
    }

    const rows = await this.adapter.find(collectionId, processedQueries);
    const result = this.processFindResults(rows, processedQueries);

    const castedResult = result.map((doc) => this.cast(collection, doc));
    const documents = await Promise.all(
      castedResult.map(async (doc) => {
        return this.filter ? await this.decode(processedQueries, doc) : doc;
      }),
    );

    this.trigger(EventsEnum.DocumentsFind, documents);

    return documents;
  }

  /**
   * find a single document.
   */
  public findOne<C extends string & keyof Entities>(
    collectionId: C,
    query?: ((builder: QueryBuilder<C>) => QueryBuilder<C>) | Query[],
  ): Promise<Doc<Entities[C]>>;
  public findOne<C extends string>(
    collectionId: C,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
  ): Promise<Doc<Partial<IEntity> & Record<string, any>>>;
  public findOne<D extends Record<string, any>>(
    collectionId: string,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
  ): Promise<Doc<Partial<IEntity>>>;
  public async findOne<C>(
    collectionId: string,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
  ): Promise<Doc> {
    const queries: Query[] = [Query.limit(1)];
    if (query && typeof query === "function") {
      queries.push(...query(new QueryBuilder()).build());
    } else {
      queries.push(...(query ?? []));
    }

    const result = await this.silent(() => this.find(collectionId, queries));
    this.trigger(EventsEnum.DocumentFind, result[0]);

    if (!result[0]) {
      return new Doc();
    }

    return result[0];
  }

  /**
   * Count documents in a collection.
   */
  public count<C extends string & keyof Entities>(
    collectionId: C,
    query?: ((builder: QueryBuilder<C>) => QueryBuilder<C>) | Query[],
    max?: number,
  ): Promise<number>;
  public count<C extends string>(
    collectionId: C,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    max?: number,
  ): Promise<number>;
  public async count(
    collectionId: string,
    query: ((builder: QueryBuilder) => QueryBuilder) | Query[] = [],
    max?: number,
  ): Promise<number> {
    const collection = await this.silent(() =>
      this.getCollection(collectionId),
    );

    const queries: Query[] =
      typeof query === "function" ? query(new QueryBuilder()).build() : query;

    const authorization = new Authorization(PermissionEnum.Read);
    let skipAuth = false;
    if (authorization.$valid(collection.getRead())) {
      skipAuth = true;
    }

    const processedQueries = await this.processQueries(queries, collection, {
      forPermission: PermissionEnum.Read,
      overrideValidators: [MethodType.Filter],
    });

    const getCount = () =>
      this.adapter.count(collection.getId(), processedQueries.filters, max);
    const count = skipAuth
      ? await Authorization.skip(getCount)
      : await getCount();

    this.trigger(EventsEnum.DocumentCount, count);

    return count;
  }

  /**
   * Sum an attribute for all documents in a collection.
   */
  public sum<C extends string & keyof Entities>(
    collectionId: C,
    attribute: keyof Entities[C] & string,
    query?: ((builder: QueryBuilder<C>) => QueryBuilder<C>) | Query[],
    max?: number,
  ): Promise<number>;
  public sum<C extends string>(
    collectionId: C,
    attribute: string,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    max?: number,
  ): Promise<number>;
  public async sum(
    collectionId: string,
    attribute: string,
    query: ((builder: QueryBuilder) => QueryBuilder) | Query[] = [],
    max?: number,
  ): Promise<number> {
    const collection = await this.silent(() =>
      this.getCollection(collectionId),
    );
    const queries: Query[] =
      typeof query === "function"
        ? query(new QueryBuilder()).build()
        : (query ?? []);

    const processedQueries = await this.processQueries(queries, collection, {
      forPermission: PermissionEnum.Read,
      overrideValidators: [MethodType.Filter],
    });

    const sum = await this.adapter.sum(
      collection.getId(),
      attribute,
      processedQueries.filters,
      max,
    );

    this.trigger(EventsEnum.DocumentSum, sum);

    return sum;
  }

  /**
   * Processes queries for a collection, validating and authorizing them.
   */
  async processQueries(
    queries: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    collection: Doc<Collection>,
    {
      forPermission = PermissionEnum.Read,
      allowedValidators = Object.values(MethodType),
      throwOnUnAuthorization = true,
      overrideValidators,
      ...metadata
    }: Partial<Attribute["options"]> & {
      populated?: boolean;
      attribute?: string;
      allowedValidators?: MethodType[];
      overrideValidators?: MethodType[];
      forPermission?: PermissionEnum;
      throwOnUnAuthorization?: boolean;
    } = {},
  ): Promise<ProcessedQuery> {
    if (typeof queries === "function") {
      queries = queries(new QueryBuilder()).build();
    }
    let authorized = true;
    let skipAuth = false;
    const validators = overrideValidators ?? allowedValidators;

    const authorizationValidator = new Authorization(forPermission);
    skipAuth = authorizationValidator.$valid(
      collection.getPermissionsByType(forPermission),
    );
    if (
      collection.getId() !== Database.METADATA &&
      !skipAuth &&
      !collection.get("documentSecurity", false)
    ) {
      if (!metadata.populated) {
        throw new AuthorizationException(authorizationValidator.$description);
      }
      if (throwOnUnAuthorization && metadata.populated) {
        throw new AuthorizationException(
          `Collection '${collection.getId()}' not authorized for '${forPermission}'. ${authorizationValidator.$description}`,
        );
      }
      authorized = false;
    }

    if (this.validate && queries.length && authorized) {
      const validator = new Documents(
        collection.get("attributes", []),
        collection.get("indexes", []),
        this.maxQueryValues,
        Object.fromEntries(validators.map((v) => [v, true])),
      );
      if (!validator.$valid(queries)) {
        throw new QueryException(validator.$description);
      }
    }

    let { populateQueries, selections, cursor, ...rest } =
      Query.groupByType(queries);
    const attributes = collection.get("attributes", []);
    const hasWildcardSelecton = selections.some((s) =>
      (s.getValues() as string[]).includes("*"),
    );

    if (selections.length > 0 && !hasWildcardSelecton) {
      const attributeMap = new Map(
        attributes.map((attr) => [attr.get("$id"), attr]),
      );

      for (const query of selections.map((s) => s.getValues()).flat()) {
        const attributeId = query as string;
        const attribute = attributeMap.get(attributeId);

        if (!attribute) {
          throw new QueryException(
            `Attribute '${attributeId}' not found in collection '${collection.getId()}'.`,
          );
        }

        const attributeType = attribute.get("type");
        if (
          attributeType === AttributeEnum.Relationship ||
          attributeType === AttributeEnum.Virtual
        ) {
          throw new QueryException(
            `Attribute '${attributeId}' of type '${attributeType}' cannot be selected directly. Use populate instead.`,
          );
        }
      }
    } else {
      selections = [
        Query.select(
          attributes
            .filter(
              (a) =>
                a.get("type") !== AttributeEnum.Relationship &&
                a.get("type") !== AttributeEnum.Virtual,
            )
            .map((a) => a.get("key", a.getId())),
        ),
      ];
    }

    if (cursor) {
      if (typeof cursor === "string") {
        cursor = (await this.silent(() =>
          this.getDocument(collection.getId(), cursor as unknown as string),
        )) as Doc<IEntity>;
      }
      if (cursor.empty()) {
        throw new NotFoundException(
          `Cursor document not found in collection '${collection.getId()}'.`,
        );
      }
      if (cursor.getCollection() !== collection.getId()) {
        throw new QueryException(
          `Cursor document must be in the same collection '${collection.getId()}'.`,
        );
      }
    }

    if (!populateQueries.size) {
      return {
        collection,
        cursor,
        selections: selections
          .map((q) => q.getValues() as unknown as string[])
          .flat(),
        populateQueries: [],
        attribute: metadata.attribute,
        authorized,
        skipAuth,
        ...rest,
      };
    }

    if (populateQueries.has("*")) {
      // TODO: Handle case where '*' is used with other populate queries like ?populate=*,author={populate: *}
      if (populateQueries.size > 1) {
        throw new QueryException(
          `Cannot use '*' with other populate queries. Use '*' alone to populate all relationships.`,
        );
      }
      populateQueries = new Map();
      for (const attribute of attributes) {
        if (attribute.get("type") === AttributeEnum.Relationship) {
          const options = attribute.get("options", {}) as RelationOptions;
          if (!options.twoWay && options.side !== RelationSideEnum.Parent)
            continue;
          populateQueries.set(attribute.get("$id"), []);
        }
      }
    }

    const processedPopulateQueries: PopulateQuery[] = [];

    for (const [attribute, values] of populateQueries.entries()) {
      const attributeDoc = attributes.find(
        (attr) => attr.get("$id") === attribute,
      );
      if (!attributeDoc) {
        throw new QueryException(
          `Attribute '${attribute}' not found in collection '${collection.getId()}'.`,
        );
      }

      if (attributeDoc.get("type") !== AttributeEnum.Relationship) {
        throw new QueryException(
          `Attribute '${attribute}' is not a relationship and cannot be populated.`,
        );
      }

      if (!Array.isArray(values)) {
        throw new QueryException(
          `Populate query for attribute '${attribute}' must be an array of queries.`,
        );
      }

      const options = attributeDoc.get("options", {}) as RelationOptions;

      if (!options.twoWay && options.side !== RelationSideEnum.Parent) {
        throw new QueryException(
          `Attribute '${attribute}' is not a parent relationship and cannot be populated.`,
        );
      }

      const relatedCollectionId = options["relatedCollection"];
      const relatedCollection = await this.silent(() =>
        this.getCollection(relatedCollectionId),
      );
      if (relatedCollection.empty()) {
        throw new QueryException(
          `Collection '${relatedCollectionId}' not found for attribute '${attribute}'.`,
        );
      }

      const processedQueries = await this.processQueries(
        values,
        relatedCollection,
        {
          populated: true,
          attribute,
          ...options,
          allowedValidators: [
            MethodType.Select,
            MethodType.Populate,
            MethodType.Filter,
            MethodType.Order,
          ],
          overrideValidators,
          throwOnUnAuthorization,
          forPermission,
        },
      );
      processedPopulateQueries.push(processedQueries as PopulateQuery);
    }

    return {
      collection,
      selections: selections
        .map((q) => q.getValues() as unknown as string[])
        .flat(),
      populateQueries: processedPopulateQueries,
      attribute: metadata.attribute,
      authorized,
      skipAuth,
      cursor,
      ...rest,
    };
  }
}

export interface ProcessedQuery
  extends Omit<QueryByType, "selections" | "populateQueries"> {
  collection: Doc<Collection>;
  selections: string[];
  populateQueries?: PopulateQuery[];
  attribute?: string;
  authorized?: boolean;
  skipAuth: boolean;
}

export type PopulateQuery = Omit<
  ProcessedQuery,
  "limit" | "offset" | "attribute"
> & {
  attribute: string;
};

export type DatabaseOptions = {
  tenant?: number;
  filters?: Filters;
};
