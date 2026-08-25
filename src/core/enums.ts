export enum AttributeEnum {
  String = "string",
  Integer = "integer",
  Float = "float",
  Boolean = "boolean",
  Timestamptz = "timestamptz",
  Json = "jsonb",
  Relationship = "relationship",
  Virtual = "virtual",
  Uuid = "uuid",
}

export enum PermissionEnum {
  Create = "create",
  Read = "read",
  Update = "update",
  Delete = "delete",
  Write = "write",
}

export enum RelationEnum {
  OneToOne = "oneToOne",
  OneToMany = "oneToMany",
  ManyToOne = "manyToOne",
  ManyToMany = "manyToMany",
}

export enum RelationSideEnum {
  Parent = "parent",
  Child = "child",
}

export enum OnDelete {
  Cascade = "cascade",
  SetNull = "setNull",
  Restrict = "restrict",
}

export enum IndexEnum {
  Unique = "unique",
  Key = "key",
  FullText = "fulltext",
  Spatial = "spatial",
}

export enum EventsEnum {
  All = "*",

  DatabaseList = "database_list",
  DatabaseCreate = "database_create",
  DatabaseDelete = "database_delete",

  CollectionList = "collection_list",
  CollectionCreate = "collection_create",
  CollectionUpdate = "collection_update",
  CollectionRead = "collection_read",
  CollectionDelete = "collection_delete",

  DocumentsFind = "documents_find",
  DocumentFind = "document_find",
  DocumentPurge = "document_purge",
  DocumentCreate = "document_create",
  DocumentsCreate = "documents_create",
  DocumentRead = "document_read",
  DocumentUpdate = "document_update",
  DocumentsUpdate = "documents_update",
  DocumentsUpsert = "documents_upsert",
  DocumentDelete = "document_delete",
  DocumentsDelete = "documents_delete",
  DocumentCount = "document_count",
  DocumentSum = "document_sum",
  DocumentIncrease = "document_increase",
  DocumentDecrease = "document_decrease",

  AttributeCreate = "attribute_create",
  AttributesCreate = "attributes_create",
  AttributeUpdate = "attribute_update",
  AttributeDelete = "attribute_delete",

  IndexRename = "index_rename",
  IndexCreate = "index_create",
  IndexDelete = "index_delete",

  RelationshipCreate = "relationship_create",
  RelationshipDelete = "relationship_delete",
  RelationshipUpdate = "relationship_update",

  /**
   * WARNING: These permission events are emitted by the adapter layer,
   * not by the Database core. Attach listeners in adapter-related code.
   */
  PermissionsCreate = "permissions_create",
  PermissionsRead = "permissions_read",
  PermissionsUpdate = "permissions_update",
  PermissionsDelete = "permissions_delete",
}

export enum CursorEnum {
  After = "after",
  Before = "before",
}

export enum OrderEnum {
  Asc = "ASC",
  Desc = "DESC",
}
