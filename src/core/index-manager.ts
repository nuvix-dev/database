/**
 * Index management operations for the Database facade.
 *
 * Extracted from the Database god class as standalone functions that receive
 * the owning Database instance explicitly. The Database class methods delegate
 * here; Database is imported type-only to avoid runtime circular imports.
 */
import type { Database } from "./database.js";
import { Base } from "./base.js";
import { EventsEnum, IndexEnum } from "./enums.js";
import { Attribute, Collection, Index } from "@validators/schema.js";
import {
  DatabaseException,
  DuplicateException,
  IndexException,
  LimitException,
  NotFoundException,
} from "@errors/index.js";
import { Index as IndexValidator } from "@validators/index-validator.js";
import { Doc } from "./doc.js";

/**
 * Update index metadata. Utility method for update index methods.
 */
export async function updateIndexMeta(
  db: Database,
  collectionId: string,
  id: string,
  updateCallback: (
    index: Doc<Index>,
    collection: Doc<Collection>,
    indexPosition: number,
  ) => void,
): Promise<Doc<Index>> {
  let collection = await db.silent(() => db.getCollection(collectionId));

  if (collection.getId() === Base.METADATA) {
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
  await db.silent(() =>
    db.system().updateDocument(Base.METADATA, collection.getId(), collection),
  );

  db.trigger(EventsEnum.AttributeUpdate, collection, indexes[indexPosition]!);

  return indexes[indexPosition]!;
}

/**
 * Renames an index in a collection.
 */
export async function renameIndex(
  db: Database,
  collectionId: string,
  oldName: string,
  newName: string,
): Promise<boolean> {
  const adapter = db.getAdapter();
  const collection = await db.silent(() => db.getCollection(collectionId));

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

  await adapter.renameIndex(collection.getId(), oldName, newName);

  if (collection.getId() !== Base.METADATA) {
    await db.silent(() =>
      db.system().updateDocument(Base.METADATA, collection.getId(), collection),
    );
  }

  db.trigger(EventsEnum.IndexRename, collection, index, oldName);

  return true;
}

/**
 * Creates an index in a collection.
 */
export async function createIndex(
  db: Database,
  validate: boolean,
  collectionId: string,
  id: string,
  type: string,
  attributes: string[],
  orders: (string | null)[] = [],
): Promise<boolean> {
  const adapter = db.getAdapter();

  if (attributes.length === 0) {
    throw new DatabaseException("Missing attributes");
  }

  const collection = await db.silent(() =>
    db.getCollection(collectionId, true),
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

  if (adapter.getCountOfIndexes(collection) >= adapter.$limitForIndexes) {
    throw new LimitException("Index limit reached. Cannot create new index.");
  }

  switch (type) {
    case IndexEnum.Key:
      if (!adapter.$supportForIndex) {
        throw new DatabaseException("Key index is not supported");
      }
      break;
    case IndexEnum.Unique:
      if (!adapter.$supportForUniqueIndex) {
        throw new DatabaseException("Unique index is not supported");
      }
      break;
    case IndexEnum.FullText:
      if (!adapter.$supportForFulltextIndex) {
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

  if (validate) {
    const validator = new IndexValidator(
      collectionAttributes,
      adapter.$maxIndexLength,
      adapter.$internalIndexesKeys,
      adapter.$supportForIndexArray,
    );
    if (!validator.$valid(index)) {
      throw new IndexException(validator.$description);
    }
  }

  try {
    const created = await adapter.createIndex({
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
      if (!adapter.$sharedTables || !db.migrating) {
        throw error;
      }
    } else {
      throw error;
    }
  }

  if (collection.getId() !== Base.METADATA) {
    await db.silent(() =>
      db.system().updateDocument(Base.METADATA, collection.getId(), collection),
    );
  }

  db.trigger(EventsEnum.IndexCreate, collection, index);

  return true;
}

/**
 * Delete an index in a collection.
 */
export async function deleteIndex(
  db: Database,
  collectionId: string,
  id: string,
): Promise<boolean> {
  const adapter = db.getAdapter();
  const collection = await db.silent(() => db.getCollection(collectionId));

  const indexes = collection.get("indexes", []);

  let indexDeleted: Doc<Index> | null = null;
  const updatedIndexes = indexes.filter((index: Doc<Index>) => {
    if (index.get("$id") === id) {
      indexDeleted = index;
      return false;
    }
    return true;
  });

  const deleted = await adapter.deleteIndex(collection.getId(), id);

  collection.set("indexes", updatedIndexes);

  if (collection.getId() !== Base.METADATA) {
    await db.silent(() =>
      db.system().updateDocument(Base.METADATA, collection.getId(), collection),
    );
  }

  db.trigger(EventsEnum.IndexDelete, collection, indexDeleted);

  return deleted;
}
