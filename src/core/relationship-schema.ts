/**
 * Relationship schema management for the Database facade.
 *
 * Extracted from the Database god class as standalone functions that receive
 * the owning Database instance explicitly. The Database class methods delegate
 * here; Database is imported type-only to avoid runtime circular imports.
 * (`Base` is imported normally because these bodies read the static `METADATA`
 * constant declared on `Base`; importing `base.js` at runtime is safe.)
 */
import type { Database } from "./database.js";
import { Base } from "./base.js";
import {
  AttributeEnum,
  EventsEnum,
  IndexEnum,
  OnDelete,
  RelationEnum,
  RelationSideEnum,
} from "./enums.js";
import type { Attribute, Index, RelationOptions } from "@validators/schema.js";
import {
  CreateRelationshipAttribute,
  UpdateRelationshipAttribute,
} from "./types.js";
import { Doc } from "./doc.js";
import {
  DatabaseException,
  DuplicateException,
  NotFoundException,
  RelationshipException,
} from "@errors/index.js";
import { updateIndexMeta } from "./index-manager.js";

/**
 * Creates a relationship between two collections.
 */
export async function createRelationship(
  db: Database,
  {
    collectionId,
    relatedCollectionId,
    type,
    twoWay = false,
    id,
    twoWayKey,
    onDelete = OnDelete.Restrict,
  }: CreateRelationshipAttribute,
): Promise<boolean> {
  const adapter = db.getAdapter();
  const collection = await db.silent(() =>
    db.getCollection(collectionId, true),
  );
  const relatedCollection = await db.silent(() =>
    db.getCollection(relatedCollectionId),
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

  db.checkAttribute(collection, relationship);
  db.checkAttribute(relatedCollection, twoWayRelationship);

  collection.append("attributes", relationship);
  relatedCollection.append("attributes", twoWayRelationship);

  if (type === RelationEnum.ManyToMany) {
    const junctionCollectionName = adapter.getJunctionTable(
      collection.getSequence(),
      relatedCollection.getSequence(),
      relationship.getId(),
      twoWayRelationship.getId(),
    );
    await db.silent(() =>
      db.createCollection({
        id: junctionCollectionName,
        attributes: [
          new Doc<Attribute>({
            $id: id,
            key: id,
            type: AttributeEnum.String,
            size: adapter.$limitForString,
            required: true,
          }),
          new Doc<Attribute>({
            $id: twoWayKey,
            key: twoWayKey,
            type: AttributeEnum.String,
            size: adapter.$limitForString,
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

  const created = await adapter.createRelationship(
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

  await db.silent(async () => {
    try {
      await db.withTransaction(async (db) => {
        await db.updateDocument(Base.METADATA, collection.getId(), collection);
        await db.updateDocument(
          Base.METADATA,
          relatedCollection.getId(),
          relatedCollection,
        );
      });
    } catch (error: any) {
      try {
        await db.silent(() => {
          if (type === RelationEnum.ManyToMany) {
            // If the relationship is ManyToMany, we need to delete the junction collection
            return adapter.deleteCollection(
              adapter.getJunctionTable(
                collection.getSequence(),
                relatedCollection.getSequence(),
                id,
                twoWayKey,
              ),
            ) as any;
          } else
            return adapter.deleteRelationship(
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
        await db.createIndex(collection.getId(), indexKey, IndexEnum.Unique, [
          id,
        ]);
        if (twoWay) {
          await db.createIndex(
            relatedCollection.getId(),
            twoWayIndexKey,
            IndexEnum.Unique,
            [twoWayKey],
          );
        }
        break;
      case RelationEnum.OneToMany:
        await db.createIndex(
          relatedCollection.getId(),
          twoWayIndexKey,
          IndexEnum.Key,
          [twoWayKey],
        );
        break;
      case RelationEnum.ManyToOne:
        await db.createIndex(collection.getId(), indexKey, IndexEnum.Key, [id]);
        break;
      case RelationEnum.ManyToMany:
        // Indexes are created during junction collection creation
        break;
      default:
        throw new DatabaseException("Invalid relationship type.");
    }
  });

  db.trigger(
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
export async function updateRelationship(
  db: Database,
  {
    collectionId,
    id,
    newKey,
    newTwoWayKey,
    twoWay,
    onDelete,
  }: UpdateRelationshipAttribute,
): Promise<boolean> {
  const adapter = db.getAdapter();

  if (!newKey && !newTwoWayKey && twoWay === undefined && !onDelete) {
    return true;
  }

  const collection = await db.silent(() =>
    db.getCollection(collectionId, true),
  );
  const attributes = collection.get("attributes", []);

  if (newKey && attributes.some((attr) => attr.get("key") === newKey)) {
    throw new DuplicateException("Relationship already exists");
  }

  const attributeIndex = attributes.findIndex((attr) => attr.get("$id") === id);
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
  const relatedCollection = await db.silent(() =>
    db.getCollection(relatedCollectionId),
  );

  await db.updateAttributeMeta(collection.getId(), id, async (attr) => {
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

    await db.updateAttributeMeta(
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
      const updated = await adapter.updateRelationship(
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
    await updateIndexMeta(db, collectionId, `_index_${key}`, (index) => {
      index.set("attributes", [newKey]);
    });
    await db.silent(() =>
      db.renameIndex(collectionId, `_index_${key}`, `_index_${newKey}`),
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
          await renameIndex(relatedCollection.getId(), twoWayKey, newTwoWayKey);
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
          await renameIndex(relatedCollection.getId(), twoWayKey, newTwoWayKey);
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

  await db.purgeCachedCollection(collection.getId());
  await db.purgeCachedCollection(relatedCollection.getId());

  return true;
}

/**
 * Deletes a relationship between two collections.
 */
export async function deleteRelationship(
  db: Database,
  collectionId: string,
  id: string,
): Promise<boolean> {
  const adapter = db.getAdapter();
  const collection = await db.silent(() => db.getCollection(collectionId));
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

  const relatedCollection = await db.silent(() =>
    db.getCollection(relatedCollectionId),
  );
  const relatedAttributes = relatedCollection.get("attributes", []);

  // Remove two-way relationship from related collection
  const updatedRelatedAttributes = relatedAttributes.filter(
    (attr) => attr.get("$id") !== twoWayKey,
  );
  relatedCollection.set("attributes", updatedRelatedAttributes);

  await db.silent(async () => {
    try {
      await db.withTransaction(async (db) => {
        await db.updateDocument(Base.METADATA, collection.getId(), collection);
        await db.updateDocument(
          Base.METADATA,
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
          await db.deleteIndex(collection.getId(), indexKey);
          if (twoWay) {
            await db.deleteIndex(relatedCollection.getId(), twoWayIndexKey);
          }
        }
        if (side === RelationSideEnum.Child) {
          await db.deleteIndex(relatedCollection.getId(), twoWayIndexKey);
          if (twoWay) {
            await db.deleteIndex(collection.getId(), indexKey);
          }
        }
        break;
      case RelationEnum.OneToMany:
        if (side === RelationSideEnum.Parent) {
          await db.deleteIndex(relatedCollection.getId(), twoWayIndexKey);
        } else {
          await db.deleteIndex(collection.getId(), indexKey);
        }
        break;
      case RelationEnum.ManyToOne:
        if (side === RelationSideEnum.Parent) {
          await db.deleteIndex(collection.getId(), indexKey);
        } else {
          await db.deleteIndex(relatedCollection.getId(), twoWayIndexKey);
        }
        break;
      case RelationEnum.ManyToMany:
        const junctionCollectionName = adapter.getJunctionTable(
          collection.getSequence(),
          relatedCollection.getSequence(),
          id,
          twoWayKey!,
        );
        await db.deleteCollection(junctionCollectionName);
        break;
      default:
        throw new RelationshipException("Invalid relationship type.");
    }
  });

  const deleted = await adapter.deleteRelationship(
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

  await db.purgeCachedCollection(collection.getId());
  await db.purgeCachedCollection(relatedCollection.getId());

  db.trigger(EventsEnum.AttributeDelete, collection, relationship);

  return true;
}
