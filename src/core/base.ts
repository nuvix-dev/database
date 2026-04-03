import { Attribute, Collection, RelationOptions } from "@validators/schema.js";
import { Emitter, EmitterEventMap } from "./emitter.js";
import {
  AttributeEnum,
  EventsEnum,
  PermissionEnum,
  RelationEnum,
  RelationSideEnum,
} from "./enums.js";
import { Cache } from "@nuvix/cache";
import { Filter, Filters } from "./types.js";
import { Meta } from "@adapters/base.js";
import { filters } from "@utils/filters.js";
import { Doc } from "./doc.js";
import {
  DatabaseException,
  DuplicateException,
  NotFoundException,
  RelationshipException,
} from "@errors/index.js";
import { Structure } from "@validators/structure.js";
import { Adapter } from "@adapters/adapter.js";
import { PopulateQuery, ProcessedQuery } from "./database.js";
import { Logger, LoggerOptions } from "@utils/logger.js";

export abstract class Base<
  T extends EmitterEventMap = EmitterEventMap,
> extends Emitter<T> {
  public static METADATA = "_metadata" as const;

  public static readonly INT_MAX = 2147483647;
  public static readonly BIG_INT_MAX = Number.MAX_SAFE_INTEGER;
  public static readonly DOUBLE_MAX = Number.MAX_VALUE;
  public static readonly ARRAY_INDEX_LENGTH = 255;
  public static readonly RELATION_MAX_DEPTH = 3;
  public static readonly LENGTH_KEY = 255;
  public static readonly TTL = 60 * 60 * 24; // 24 hours
  public static readonly INSERT_BATCH_SIZE = 1000;
  public static readonly DELETE_BATCH_SIZE = 1000;
  public static readonly DEFAULT_BATCH_SIZE = 1000;
  public static readonly FULLTEXT_LANGUAGE = "english";

  public static readonly INTERNAL_ATTRIBUTES: Attribute[] = [
    {
      $id: "$id",
      key: "$id",
      type: AttributeEnum.String,
      size: Base.LENGTH_KEY,
      required: true,
    },
    {
      $id: "$sequence",
      key: "$sequence",
      type: AttributeEnum.Integer,
      size: 8,
    },
    {
      $id: "$collection",
      key: "$collection",
      type: AttributeEnum.String,
      size: Base.LENGTH_KEY,
      required: true,
    },
    {
      $id: "$schema",
      key: "$schema",
      type: AttributeEnum.String,
      size: Base.LENGTH_KEY,
      required: false,
    },
    {
      $id: "$tenant",
      key: "$tenant",
      type: AttributeEnum.Integer,
      size: 8,
    },
    {
      $id: "$createdAt",
      key: "$createdAt",
      type: AttributeEnum.Timestamptz,
      default: null,
    },
    {
      $id: "$updatedAt",
      key: "$updatedAt",
      type: AttributeEnum.Timestamptz,
      default: null,
    },
    {
      $id: "$permissions",
      key: "$permissions",
      type: AttributeEnum.String,
      size: 255,
      array: true,
    },
  ];
  public static readonly INTERNAL_ATTRIBUTE_KEYS = [
    "_uid",
    "_createdAt",
    "_updatedAt",
    "_permissions",
  ];
  public static readonly INTERNAL_INDEXES: string[] = [
    "_id",
    "_uid",
    "_createdAt",
    "_updatedAt",
    "_permissions_id",
    "_permissions",
  ];
  protected static readonly COLLECTION: Collection = {
    $id: Base.METADATA,
    $collection: Base.METADATA,
    name: "collections",
    attributes: [
      {
        $id: "name",
        key: "name",
        type: AttributeEnum.String,
        size: 256,
        required: true,
      },
      {
        $id: "attributes",
        key: "attributes",
        type: AttributeEnum.Json,
      },
      {
        $id: "indexes",
        key: "indexes",
        type: AttributeEnum.Json,
      },
      {
        $id: "documentSecurity",
        key: "documentSecurity",
        type: AttributeEnum.Boolean,
        required: true,
      },
      {
        $id: "enabled",
        key: "enabled",
        type: AttributeEnum.Boolean,
        default: true,
      },
    ],
    indexes: [],
    enabled: true,
    documentSecurity: false,
  };

  public static readonly PERMISSIONS: PermissionEnum[] = [
    PermissionEnum.Create,
    PermissionEnum.Read,
    PermissionEnum.Update,
    PermissionEnum.Delete,
  ];

  protected readonly adapter: Adapter;
  protected readonly cache: Cache;

  protected static filters: Filters = {};
  protected readonly instanceFilters: Filters;
  protected timestamp?: Date;
  protected filter: boolean = true;
  protected validate: boolean = true;
  protected preserveDates: boolean = false;
  protected maxQueryValues: number = 100;
  protected globalCollections: Record<string, boolean> = {};
  protected resolveRelationships: boolean = true;
  protected checkRelationshipsExist: boolean = true;
  protected isMigrating: boolean = false;
  protected readonly _relationStack: Set<string> = new Set();
  protected _collectionEnabledValidate: boolean = false;
  protected attachSchemaInDocument: boolean = true;

  protected readonly logger: Logger;

  constructor(adapter: Adapter, cache: Cache, options: Options = {}) {
    super();
    this.adapter = adapter;
    this.cache = cache;
    this.instanceFilters = options.filters || {};
    if (options.logger) {
      this.logger =
        options.logger instanceof Logger
          ? options.logger
          : new Logger(options.logger);
    } else {
      this.logger = new Logger();
    }
    this.adapter.setLogger(this.logger);

    for (const [filterName, FilterValue] of Object.entries(filters)) {
      Base.filters[filterName] = FilterValue as Filter;
    }
  }

  public addFilter(name: string, filter: Filter): this {
    if (this.instanceFilters[name]) {
      throw new Error(`Filter with name "${name}" already exists.`);
    }
    this.instanceFilters[name] = filter;
    return this;
  }

  public static addFilter(name: string, filter: Filter): void {
    if (Base.filters[name]) {
      throw new Error(`Filter with name "${name}" already exists.`);
    }
    Base.filters[name] = filter;
  }

  public getFilters(): Filters {
    return { ...Base.filters, ...this.instanceFilters };
  }

  public getAdapter(): Adapter {
    return this.adapter;
  }

  public enableFilters(): this {
    this.filter = true;
    return this;
  }

  public disableFilters(): this {
    this.filter = false;
    return this;
  }

  public enableValidation(): this {
    this.validate = true;
    return this;
  }

  public disableValidation(): this {
    this.validate = false;
    return this;
  }

  public setMeta(meta: Partial<Meta>): this {
    this.adapter.setMeta(meta);
    return this;
  }

  public get database() {
    return this.adapter.$database;
  }

  public get schema() {
    return this.adapter.$schema;
  }

  public get sharedTables(): boolean {
    return this.adapter.$sharedTables;
  }

  public get migrating(): boolean {
    return this.isMigrating;
  }

  public get tenantId(): number | undefined {
    return this.adapter.$tenantId;
  }

  public get tenantPerDocument(): boolean {
    return this.adapter.$tenantPerDocument;
  }

  public get namespace(): string {
    return this.adapter.$namespace;
  }

  public get metadata() {
    return this.adapter.$metadata;
  }

  public get preserveDatesEnabled(): boolean {
    return this.preserveDates;
  }

  public get collectionEnabledValidate(): boolean {
    return this._collectionEnabledValidate;
  }

  public setCollectionEnabledValidate(value: boolean): this {
    this._collectionEnabledValidate = value;
    return this;
  }

  public setPreserveDates(preserve: boolean): this {
    this.preserveDates = preserve;
    return this;
  }

  public setAttachSchemaInDocument(value: boolean): this {
    this.attachSchemaInDocument = value;
    return this;
  }

  public before(
    event: EventsEnum,
    name: string,
    callback?: (query: string) => string,
  ) {
    this.adapter.before(event, name, callback);
    return this;
  }

  public async withRequestTimestamp<T>(
    requestTimestamp: Date | null,
    callback: Callback<T>,
  ): Promise<T> {
    const previous = this.timestamp;
    this.timestamp = requestTimestamp ?? undefined;
    try {
      return await callback();
    } finally {
      this.timestamp = previous;
    }
  }

  public async skipFilters<T>(callback: Callback<T>): Promise<T> {
    const initial = this.filter;
    this.disableFilters();

    try {
      return await callback();
    } finally {
      this.filter = initial;
    }
  }

  public async skipValidation<T>(callback: Callback<T>): Promise<T> {
    const initial = this.validate;
    this.disableValidation();

    try {
      return await callback();
    } finally {
      this.validate = initial;
    }
  }

  public async skipRelationships<T>(callback: Callback<T>): Promise<T> {
    const previous = this.resolveRelationships;
    this.resolveRelationships = false;

    try {
      return callback();
    } finally {
      this.resolveRelationships = previous;
    }
  }

  public async skipCheckRelationshipsExist<T>(
    callback: Callback<T>,
  ): Promise<T> {
    const previous = this.checkRelationshipsExist;
    this.checkRelationshipsExist = false;

    try {
      return callback();
    } finally {
      this.checkRelationshipsExist = previous;
    }
  }

  public async withTenant<T>(
    tenantId: number | null,
    callback: Callback<T>,
  ): Promise<T> {
    const previous = this.adapter.$tenantId;
    this.adapter.setMeta({ tenantId: tenantId ?? undefined });

    try {
      return await callback();
    } finally {
      this.adapter.setMeta({ tenantId: previous });
    }
  }

  public async withSchema<T>(
    schema: string,
    callback: Callback<T>,
  ): Promise<T> {
    const previous = this.adapter.$schema;
    this.adapter.setMeta({ schema });

    try {
      return await callback();
    } finally {
      this.adapter.setMeta({ schema: previous });
    }
  }

  public async withPreserveDates<T>(callback: Callback<T>): Promise<T> {
    const previous = this.preserveDates;
    this.preserveDates = true;

    try {
      return await callback();
    } finally {
      this.preserveDates = previous;
    }
  }

  public async withCollectionEnabledValidation<T>(
    enabled: boolean,
    callback: Callback<T>,
  ): Promise<T> {
    const previous = this._collectionEnabledValidate;
    this._collectionEnabledValidate = enabled;

    try {
      return await callback();
    } finally {
      this._collectionEnabledValidate = previous;
    }
  }

  public async withAttachSchemaInDocument<T>(
    enabled: boolean,
    callback: Callback<T>,
  ): Promise<T> {
    const previous = this.attachSchemaInDocument;
    this.attachSchemaInDocument = enabled;

    try {
      return await callback();
    } finally {
      this.attachSchemaInDocument = previous;
    }
  }

  async withTransaction<T>(
    callback: (txDatabase: this) => Promise<T>,
  ): Promise<T> {
    return await this.adapter.transaction(async (txAdapter) => {
      if (this.adapter === txAdapter) {
        return await callback(this);
      }

      const txDatabase = Object.create(Object.getPrototypeOf(this));
      Object.assign(txDatabase, this);
      txDatabase.adapter = txAdapter;

      return await callback(txDatabase);
    });
  }

  public get ping() {
    return this.adapter.ping();
  }

  protected getJunctionTable(
    coll: number,
    relColl: number,
    attr: string,
    relAttr: string,
  ): string {
    return this.adapter.getJunctionTable(coll, relColl, attr, relAttr);
  }

  protected async validateAttribute(
    collection: Doc<Collection>,
    attribute: Attribute,
  ): Promise<Doc<Attribute>> {
    const attributes = collection.get("attributes", []);
    const key = attribute.key;
    attributes.forEach((attr) => {
      if (attr.get("key").toLowerCase() === key.toLowerCase()) {
        throw new DuplicateException(
          `Attribute '${key}' already exists in metadata`,
        );
      }
    });

    if (!(this.sharedTables && this.migrating)) {
      for (const attr of await this.adapter.getSchemaAttributes(
        collection.getId(),
      )) {
        if (
          this.adapter.sanitize(attr.getId()).toLowerCase() ===
          key.toLowerCase()
        ) {
          throw new DuplicateException(
            `Attribute '${key}' already exists in schema`,
          );
        }
      }
    }

    const type = attribute.type;
    const filtersList = attribute.filters ?? [];
    const format = attribute.format ?? null;
    const size = attribute.size ?? 0;
    const defaultValue = attribute.default ?? null;

    const requiredFilters = this.getRequiredFilters(type);
    if (requiredFilters.length > 0) {
      const missingFilters = requiredFilters.filter(
        (f) => !filtersList.includes(f),
      );
      if (missingFilters.length > 0) {
        throw new DatabaseException(
          `Attribute of type: ${type} requires the following filters: ${missingFilters.join(",")}`,
        );
      }
    }

    if (format && !Structure.hasFormat(format, type)) {
      throw new DatabaseException(
        `Format ("${format}") not available for this attribute type ("${type}")`,
      );
    }

    const attr = new Doc(attribute);
    this.checkAttribute(collection, attr);

    switch (type) {
      case AttributeEnum.String:
        if (size > this.adapter.$limitForString) {
          throw new DatabaseException(
            `Max size allowed for string is: ${this.adapter.$limitForString}`,
          );
        }
        break;
      case AttributeEnum.Integer:
        if (size > this.adapter.$limitForInt) {
          throw new DatabaseException(
            `Max size allowed for int is: ${this.adapter.$limitForInt}`,
          );
        }
        break;
      case AttributeEnum.Float:
      case AttributeEnum.Boolean:
      case AttributeEnum.Timestamptz:
      case AttributeEnum.Json:
      case AttributeEnum.Relationship:
      case AttributeEnum.Virtual:
      case AttributeEnum.Uuid:
        break;
      default:
        throw new DatabaseException(
          `Unknown attribute type: ${type}, Must be one of ${Object.values(AttributeEnum)}`,
        );
    }

    if (defaultValue !== null) {
      if (attribute.required === true) {
        throw new DatabaseException(
          "Cannot set a default value for a required attribute",
        );
      }
      this.validateDefaultTypes(type, defaultValue);
    }
    return attr;
  }

  /**
   * Checks if attribute can be added to collection.
   * Used to check attribute limits without asking the database
   * Returns true if attribute can be added to collection, throws exception otherwise
   */
  public checkAttribute(
    collection: Doc<Collection>,
    attribute: Doc<Attribute>,
  ): boolean {
    const clonedCollection = collection.clone();
    clonedCollection.append("attributes", attribute);

    const attributeLimit = this.adapter.$limitForAttributes;
    if (
      attributeLimit > 0 &&
      this.adapter.getCountOfAttributes(clonedCollection) > attributeLimit
    ) {
      throw new DatabaseException(
        "Column limit reached. Cannot create new attribute.",
      );
    }

    const documentSizeLimit = this.adapter.$documentSizeLimit;
    if (
      documentSizeLimit > 0 &&
      this.adapter.getAttributeWidth(clonedCollection) >= documentSizeLimit
    ) {
      throw new DatabaseException(
        "Row width limit reached. Cannot create new attribute.",
      );
    }

    return true;
  }

  protected validateDefaultTypes(type: AttributeEnum, value: unknown): void {
    if (value === null || value === undefined) {
      // Disable null. No validation required
      return;
    }

    if (Array.isArray(value)) {
      for (const v of value) {
        this.validateDefaultTypes(type, v);
      }
      return;
    }

    const valueType = typeof value;

    switch (type) {
      case AttributeEnum.Json:
        if (valueType !== "object") {
          throw new DatabaseException(
            `Default value ${value} does not match given type ${type}`,
          );
        }
        break;
      case AttributeEnum.Uuid:
      case AttributeEnum.String:
        if (valueType !== "string") {
          throw new DatabaseException(
            `Default value ${value} does not match given type ${type}`,
          );
        }
        break;
      case AttributeEnum.Integer:
        if (valueType !== "number" || !Number.isInteger(value)) {
          throw new DatabaseException(
            `Default value ${value} does not match given type ${type}`,
          );
        }
        break;
      case AttributeEnum.Float:
        if (valueType !== "number") {
          throw new DatabaseException(
            `Default value ${value} does not match given type ${type}`,
          );
        }
        break;
      case AttributeEnum.Boolean:
        if (valueType !== "boolean") {
          throw new DatabaseException(
            `Default value ${value} does not match given type ${type}`,
          );
        }
        break;
      case AttributeEnum.Timestamptz:
        if (valueType !== "string") {
          throw new DatabaseException(
            `Default value ${value} does not match given type ${type}`,
          );
        }
        break;
      default:
        throw new DatabaseException(
          `Unknown attribute type: ${type}. Must be one of ${Object.values(AttributeEnum)}`,
        );
    }
  }

  protected getRequiredFilters(type: AttributeEnum): string[] {
    switch (type) {
      default:
        return [];
    }
  }

  protected assertCollectionEnabled(collection: Doc<Collection>): boolean {
    if (!this._collectionEnabledValidate) return false;

    if (!collection) {
      throw new DatabaseException("Collection is required");
    }

    const enabled = collection.get("enabled", true);

    if (!enabled) {
      this.logger.info(`Collection '${collection.getId()}' is disabled`);
      return true;
    }

    return false;
  }

  protected cast<T extends Record<string, any>>(
    collection: Doc<Collection>,
    document: Doc<T>,
  ): Doc<T> {
    if (this.adapter.$supportForCasting) {
      return document;
    }

    const attributes: (Attribute | Doc<Attribute>)[] =
      collection.get("attributes") ?? [];
    for (const attribute of Base.INTERNAL_ATTRIBUTES) {
      attributes.push(attribute);
    }

    for (const attr of attributes) {
      const attribute = attr instanceof Doc ? attr.toObject() : attr;
      const key = attribute.$id ?? "";
      const type = attribute.type ?? "";
      const array = attribute.array ?? false;
      const value = document.get(key, null);

      if (value === null || value === undefined) {
        continue;
      }

      let processedValue: any;
      if (array) {
        processedValue = typeof value === "string" ? JSON.parse(value) : value;
      } else {
        processedValue = [value];
      }

      for (let index = 0; index < processedValue.length; index++) {
        let node = processedValue[index];

        switch (type) {
          case AttributeEnum.Boolean:
            node = Boolean(node);
            break;
          case AttributeEnum.Integer:
            node = parseInt(node, 10);
            break;
          case AttributeEnum.Float:
            node = parseFloat(node);
            break;
          default:
            break;
        }

        processedValue[index] = node;
      }

      document.set(key, array ? processedValue : processedValue[0]);
    }
    return document;
  }

  protected async encode<T extends Record<string, any>>(
    collection: Doc<Collection>,
    document: Doc<T>,
  ): Promise<Doc<T>> {
    const attributes: (Attribute | Doc<Attribute>)[] = [
      ...(collection.get("attributes") ?? []),
      ...this.getInternalAttributes(),
    ];
    const internalDateAttributes = ["$createdAt", "$updatedAt"];

    for (const attr of attributes) {
      const attribute = attr instanceof Doc ? attr.toObject() : attr;
      const key = attribute.$id ?? "";
      const array = attribute.array ?? false;
      const defaultValue = attribute.default ?? null;
      const attributeFilters = attribute.filters ?? [];
      let value: any = document.get(key);

      if (attribute.type === AttributeEnum.Virtual || key === "$schema") {
        document.delete(key);
        continue;
      }

      if (attribute.type === AttributeEnum.Relationship) continue;
      if (
        internalDateAttributes.includes(key) &&
        typeof value === "string" &&
        value === ""
      ) {
        document.set(key, null);
        continue;
      }

      if (key === "$permissions") {
        if (!value) {
          document.set("$permissions", []);
        }
        continue;
      }

      // Continue on optional param with no default
      if (value === null && defaultValue === null) {
        continue;
      }

      // Assign default only if no value provided
      if (value === null && defaultValue !== null) {
        value = array ? defaultValue : [defaultValue];
      } else {
        value = array ? value : [value];
      }

      if (
        attribute.type === AttributeEnum.Timestamptz &&
        typeof value === "string"
      ) {
        value = new Date(value).toISOString();
      }

      for (let index = 0; index < value.length; index++) {
        let node = value[index];
        if (node !== null) {
          for (const filter of attributeFilters) {
            node = await this.encodeAttribute(
              filter,
              node,
              document as unknown as Doc,
            );
          }
          value[index] = node;
        }
      }

      if (!array) {
        value = value[0];
      }
      if (attribute.type === AttributeEnum.Json && typeof value === "object") {
        value = JSON.stringify(value);
      }
      document.set(key, value);
    }

    return document;
  }

  protected async decode<T extends Record<string, any>>(
    {
      collection,
      populateQueries,
    }: Pick<ProcessedQuery | PopulateQuery, "collection" | "populateQueries">,
    document: Doc<Record<string, any>>,
  ): Promise<Doc<T>> {
    const internalAttributes = this.getInternalAttributes();
    const attributes = [
      ...(collection.get("attributes") ?? []),
      ...internalAttributes,
    ].map((attr) => (attr instanceof Doc ? attr.toObject() : attr));

    for (const attribute of attributes) {
      if (attribute.type !== AttributeEnum.Relationship || !attribute.$id)
        continue;

      const originalKey = attribute.$id;
      const sanitizedKey = this.adapter.sanitize(originalKey);

      if (originalKey !== sanitizedKey && document.has(sanitizedKey)) {
        const value = document.get(sanitizedKey);
        if (!document.has(originalKey) || document.get(originalKey) == null) {
          document.set(originalKey, value);
        }
        document.delete(sanitizedKey);
      }
    }

    // Decode filters for non-Relationship attributes
    for (const attribute of attributes) {
      const key = attribute.$id;
      if (!key) continue;

      // Remove relationship attributes from the document when no population queries are provided,
      // as the document may contain direct IDs instead of fully populated related documents.
      if (attribute.type === AttributeEnum.Relationship) {
        if (
          !populateQueries ||
          (populateQueries.length === 0 && document.has(key))
        ) {
          document.delete(key);
        }
        continue;
      }

      if (!document.has(key) && attribute.type !== AttributeEnum.Virtual)
        continue;

      const isArray = attribute.array ?? false;
      const filters = attribute.filters ?? [];

      let values = document.get(key);
      const items =
        attribute.type === AttributeEnum.Json
          ? values && typeof values === "string"
            ? [JSON.parse(values)]
            : [values] // TODO: ------------
          : Array.isArray(values)
            ? values
            : values != null || attribute.type === AttributeEnum.Virtual
              ? [values]
              : [];

      const processed = await Promise.all(
        items.map(async (item) => {
          for (const filter of [...filters].reverse()) {
            item = await this.decodeAttribute(
              filter,
              item,
              document as unknown as Doc,
              key,
            );
          }
          return item;
        }),
      );

      document.set(key, isArray ? processed : (processed[0] ?? null));
    }

    if (!this.attachSchemaInDocument) {
      document.delete("$schema");
    } else if (!document.has("$schema")) {
      document.set("$schema", this.schema);
    }

    // Decode population (relationships)
    if (populateQueries?.length) {
      const relationshipAttrMap = new Map(
        (collection.get("attributes") ?? [])
          .filter((attr) => attr.get("type") === AttributeEnum.Relationship)
          .map((attr) => [attr.getId(), attr]),
      );
      await Promise.all(
        populateQueries.map(async (populateQuery) => {
          const attribute = relationshipAttrMap.get(populateQuery.attribute);
          if (!attribute) return;

          const key = attribute.get("key", attribute.getId());
          const options = attribute.get("options", {}) as RelationOptions;
          const relationType = options.relationType;
          const side = options.side;
          let value = document.get(key, null);

          if (
            (relationType === RelationEnum.ManyToOne &&
              side === RelationSideEnum.Child) ||
            (relationType === RelationEnum.OneToMany &&
              side === RelationSideEnum.Parent) ||
            relationType === RelationEnum.ManyToMany
          ) {
            value ??= [];
            const resolved = await Promise.all(
              value.map((v: any) => this.decode(populateQuery, v)),
            );
            document.set(key, resolved);
          } else {
            document.set(
              key,
              value != null ? await this.decode(populateQuery, value) : null,
            );
          }
        }),
      );
    }

    return document as Doc<T>;
  }

  private async encodeAttribute(
    filter: string,
    value: any,
    document: Doc,
  ): Promise<any> {
    const allFilters = this.getFilters();

    if (!allFilters[filter]) {
      throw new NotFoundException(`Filter: ${filter} not found`);
    }

    try {
      if (this.instanceFilters[filter]) {
        value = this.instanceFilters[filter].encode(
          value,
          document,
          this as any,
        );
      } else {
        value = Base.filters[filter]!.encode(value, document, this as any);
      }
      if (value instanceof Promise) {
        value = await value;
      }
    } catch (error) {
      throw new DatabaseException(
        error instanceof Error ? error.message : String(error),
      );
    }

    return value;
  }

  private async decodeAttribute(
    filter: string,
    value: any,
    document: Doc,
    attribute: string,
  ): Promise<any> {
    if (!this.filter) {
      return value;
    }

    const allFilters = this.getFilters();
    if (!allFilters[filter]) {
      throw new NotFoundException(
        `Filter "${filter}" not found for attribute "${attribute}"`,
      );
    }

    try {
      if (this.instanceFilters[filter]) {
        value = this.instanceFilters[filter].decode(
          value,
          document,
          this as any,
        );
      } else {
        value = Base.filters[filter]!.decode(value, document, this as any);
      }
      if (value instanceof Promise) {
        value = await value;
      }
    } catch (error) {
      throw new DatabaseException(
        error instanceof Error ? error.message : String(error),
      );
    }

    return value;
  }

  public getInternalAttributes(): Attribute[] {
    let attributes = Base.INTERNAL_ATTRIBUTES;

    if (!this.sharedTables) {
      attributes = Base.INTERNAL_ATTRIBUTES.filter(
        (attribute) => attribute.$id !== "$tenant",
      );
    }

    return attributes;
  }

  /**
   * Processes the results from Find to group related data properly
   */
  protected processFindResults(
    rows: any[],
    { ...query }: ProcessedQuery,
  ): any[] {
    if (!rows.length) return [];

    const documentsMap = new Map<string, any>();
    const internalAttrs = this.getInternalAttributes().map((a) => a.$id);
    const selections = [
      ...internalAttrs,
      "$schema",
      "$collection",
      ...query.selections,
    ];

    // Group rows by main document ID
    for (const row of rows) {
      const mainId = row["$id"] || row["$sequence"];
      if (!mainId) continue;

      if (!documentsMap.has(mainId)) {
        const mainDoc: Record<string, any> = {};

        for (const attr of selections) {
          switch (attr) {
            case "$schema":
              if (!this.attachSchemaInDocument) break;
              mainDoc[attr] = this.schema;
              break;
            case "$collection":
              mainDoc[attr] = query.collection.getId();
              break;
            default:
              mainDoc[attr] = row[attr];
          }
        }

        if (query.populateQueries?.length) {
          this.initializeRelationships(
            mainDoc,
            query.populateQueries,
            query.collection,
          );
        }

        documentsMap.set(mainId, mainDoc);
      }

      if (query.populateQueries?.length) {
        this.processPopulatedData(
          documentsMap.get(mainId)!,
          query.collection,
          row,
          query.populateQueries,
          0,
        );
      }
    }

    return Array.from(documentsMap.values()).map((doc) => new Doc(doc));
  }

  /**
   * Initialize relationship fields in the main document
   */
  private initializeRelationships(
    document: Record<string, any>,
    populateQueries: PopulateQuery[],
    collection: Doc<Collection>,
  ): void {
    for (const populateQuery of populateQueries) {
      const relationshipAttr = collection
        .get("attributes", [])
        .find(
          (attr) =>
            attr.get("type") === AttributeEnum.Relationship &&
            attr.get("key", attr.getId()) === populateQuery.attribute,
        );

      if (relationshipAttr) {
        const options = relationshipAttr.get("options", {}) as RelationOptions;
        const relationType = options.relationType;
        const side = options.side;
        const relationshipKey = relationshipAttr.get(
          "key",
          relationshipAttr.getId(),
        );

        if (
          (relationType === RelationEnum.OneToMany &&
            side === RelationSideEnum.Parent) ||
          (relationType === RelationEnum.ManyToOne &&
            side === RelationSideEnum.Child) ||
          relationType === RelationEnum.ManyToMany
        ) {
          document[relationshipKey] = [];
        } else {
          document[relationshipKey] = null;
        }
      }
    }
  }

  /**
   * Recursively processes populated relationship data
   */
  private processPopulatedData(
    document: Record<string, any>,
    collection: Doc<Collection>,
    row: Record<string, any>,
    populate: PopulateQuery[],
    depth: number,
    parentPrefix: string = "",
  ): void {
    const internalAttrs = this.getInternalAttributes().map((a) => a.$id);
    for (let i = 0; i < populate.length; i++) {
      const { attribute, ...populateQuery }: PopulateQuery = populate[i]!;
      const relationshipAttr = collection
        .get("attributes", [])
        .find(
          (attr) =>
            attr.get("type") === AttributeEnum.Relationship &&
            attr.get("key", attr.getId()) === attribute,
        );

      if (!relationshipAttr) continue;

      const options = relationshipAttr.get("options", {}) as RelationOptions;
      const relationshipKey = relationshipAttr.get(
        "key",
        relationshipAttr.getId(),
      );
      const relationType = options.relationType;
      const side = options.side;

      const currentPrefix = parentPrefix
        ? `${parentPrefix}_${relationshipKey}_`
        : `${relationshipKey}_`;
      const selections = [
        ...internalAttrs,
        "$schema",
        "$collection",
        ...populateQuery.selections,
      ];

      const relatedDoc: Record<string, any> = {};
      let hasRelatedData = false;

      for (const attr of selections) {
        if (attr === "$collection")
          relatedDoc[attr] = populateQuery.collection.getId();
        else if (attr === "$schema") {
          if (!this.attachSchemaInDocument) continue;
          relatedDoc[attr] = this.schema;
        } else {
          const key = `${currentPrefix}${attr}`;
          const value = row[key];
          if (value !== null && value !== undefined) {
            hasRelatedData = true;
          }
          relatedDoc[attr] = value;
        }
      }

      if (hasRelatedData) {
        if (
          populateQuery.populateQueries &&
          populateQuery.populateQueries.length > 0
        ) {
          this.initializeRelationships(
            relatedDoc,
            populateQuery.populateQueries,
            populateQuery.collection,
          );
          this.processPopulatedData(
            relatedDoc,
            populateQuery.collection,
            row,
            populateQuery.populateQueries,
            depth + 1,
            currentPrefix.slice(0, -1),
          );
        }

        if (
          (relationType === RelationEnum.OneToMany &&
            side === RelationSideEnum.Parent) ||
          (relationType === RelationEnum.ManyToOne &&
            side === RelationSideEnum.Child) ||
          relationType === RelationEnum.ManyToMany
        ) {
          // Array relationship
          document[relationshipKey] ??= [];
          const relatedId = relatedDoc["$id"] || relatedDoc["$sequence"];
          if (
            relatedId &&
            !document[relationshipKey].some(
              (item: any) => (item["$id"] || item["$sequence"]) === relatedId,
            )
          ) {
            document[relationshipKey].push(relatedDoc);
          }
        } else {
          // Single relationship
          if (!document[relationshipKey]) {
            document[relationshipKey] = relatedDoc;
          }
        }
      }
    }
  }

  protected formatRelationValue(value: any): {
    setIds: string[] | null | undefined;
    connectIds: string[];
    disconnectIds: string[];
  } {
    let setIds: string[] | null | undefined = undefined;
    const connectIdsSet = new Set<string>();
    const disconnectIdsSet = new Set<string>();

    if (typeof value !== "object" && value !== null) {
      throw new RelationshipException(
        "Invalid value for relationship: must be an object or null",
      );
    }

    if (value === null) {
      // Null means "clear all relationships"
      setIds = null;
    } else {
      if ("connect" in value) {
        const connectValues = value.connect;
        if (connectValues !== undefined) {
          if (!Array.isArray(connectValues)) {
            throw new RelationshipException(
              "Connect must be an array of string IDs",
            );
          }
          for (const id of connectValues) {
            if (typeof id !== "string") {
              throw new RelationshipException("Ids in connect must be strings");
            }
            connectIdsSet.add(id);
          }
        }
      }

      if ("disconnect" in value) {
        const disconnectValues = value.disconnect;
        if (disconnectValues !== undefined) {
          if (!Array.isArray(disconnectValues)) {
            throw new RelationshipException(
              "Disconnect must be an array of string IDs",
            );
          }
          for (const id of disconnectValues) {
            if (typeof id !== "string") {
              throw new RelationshipException(
                "Ids in disconnect must be strings",
              );
            }
            disconnectIdsSet.add(id);
          }
        }
      }

      if ("set" in value) {
        const setValues = value.set;
        if (setValues === null) {
          setIds = null; // Explicit null = clear all
        } else if (setValues !== undefined) {
          if (!Array.isArray(setValues)) {
            throw new RelationshipException(
              "Set must be an array of string IDs or null",
            );
          }
          setIds = Array.from(
            new Set(
              setValues.map((id) => {
                if (typeof id !== "string") {
                  throw new RelationshipException("Ids in set must be strings");
                }
                return id;
              }),
            ),
          );
        }
      }
    }

    const connectIds = Array.from(connectIdsSet);
    const disconnectIds = Array.from(disconnectIdsSet);

    if (
      setIds !== undefined &&
      (connectIds.length > 0 || disconnectIds.length > 0)
    ) {
      throw new RelationshipException(
        "Cannot use set with connect or disconnect at the same time.",
      );
    }

    return { setIds, connectIds, disconnectIds };
  }
}

type Options = {
  tenant?: number;
  filters?: Filters;
  logger?: LoggerOptions | Logger;
};

type Callback<T> = () => Promise<T> | T;
