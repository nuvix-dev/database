/**
 * Document-level relationship maintenance for the Database facade.
 *
 * Extracted from the Database god class as standalone functions that receive
 * the owning Database instance explicitly. The Database class methods delegate
 * here; Database is imported type-only to avoid runtime circular imports.
 *
 * These handlers run inside create/update/delete document flows and perform
 * internal relationship bookkeeping. Authorization.skip-wrapped blocks below
 * are security-critical, regression-tested behavior (WS-F1/F2): they bypass
 * target-collection authorization ONLY for deterministic schema maintenance
 * (junction-table writes, twoWay OneToOne clear/set, cascade/set-null/restrict
 * enforcement) and must be preserved byte-for-byte.
 */
import type { Database } from "./database.js";
import {
  AttributeEnum,
  OnDelete,
  RelationEnum,
  RelationSideEnum,
} from "./enums.js";
import type {
  Attribute,
  Collection,
  RelationOptions,
} from "@validators/schema.js";
import { Doc } from "./doc.js";
import { DatabaseException, RelationshipException } from "@errors/index.js";
import { Authorization } from "@utils/authorization.js";
import { Query } from "./query.js";
import { ID } from "@utils/id.js";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";
import { formatRelationValue } from "./validation-utils.js";

/**
 * Create relationships for a document being created.
 */
export async function createRelationships(
  db: Database,
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
    if (db.relationStack.has(loopKey)) continue;
    db.relationStack.add(loopKey);

    try {
      if (
        type === RelationEnum.OneToOne ||
        (type === RelationEnum.OneToMany && side === RelationSideEnum.Child) ||
        (type === RelationEnum.ManyToOne && side === RelationSideEnum.Parent)
      ) {
        const relatedDoc = await db.silent(() =>
          db.getDocument(options.relatedCollection, value),
        );

        if (relatedDoc.empty() && !db.checksRelationshipsExist) {
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
            await db.silent(() =>
              db.skipCheckRelationshipsExist(() =>
                db.updateDocument(options.relatedCollection, value, relatedDoc),
              ),
            );
          }
        }
      }

      if (
        type === RelationEnum.ManyToMany ||
        (type === RelationEnum.OneToMany && side === RelationSideEnum.Parent) ||
        (type === RelationEnum.ManyToOne && side === RelationSideEnum.Child)
      ) {
        const { setIds } = formatRelationValue(value);
        if (!setIds) continue;
        if (type === RelationEnum.ManyToMany) {
          await handleManyToMany(
            db,
            collection,
            document,
            relationship,
            options,
            setIds,
          );
        } else {
          for (const childId of setIds) {
            const childDoc = await db.silent(() =>
              db.getDocument(options.relatedCollection, childId),
            );
            if (childDoc.empty() && !db.checksRelationshipsExist) {
              throw new RelationshipException(`Child '${childId}' not found`);
            }
            childDoc.set(options.twoWayKey!, document.getId());
            await db.silent(() =>
              db.skipCheckRelationshipsExist(() =>
                db.updateDocument(options.relatedCollection, childId, childDoc),
              ),
            );
          }
        }
        document.delete(relationship.get("key"));
      }
    } finally {
      db.relationStack.delete(loopKey);
    }
  }
  return document;
}

/**
 * Many-to-Many handling
 */
export async function handleManyToMany(
  db: Database,
  collection: Doc<Collection>,
  document: Doc<any>,
  relationship: Doc<Attribute>,
  options: RelationOptions,
  setIds: string[] | null | undefined = undefined,
  connectIds: string[] = [],
  disconnectIds: string[] = [],
): Promise<void> {
  const adapter = db.getAdapter();

  // Skip if nothing to do
  if (
    setIds === undefined &&
    connectIds.length === 0 &&
    disconnectIds.length === 0
  ) {
    return;
  }

  const relatedCollection = await db.silent(() =>
    db.getCollection(options.relatedCollection, true),
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
  const junctionCollection = adapter.getJunctionTable(
    parentColl.getSequence(),
    childColl.getSequence(),
    parentAttr,
    childAttr,
  );

  if (setIds !== undefined) {
    await Authorization.skip(() =>
      db.silent(() =>
        db.deleteDocuments(junctionCollection, [
          Query.equal(relationship.getId(), [document.getId()]),
        ]),
      ),
    );
  } else if (disconnectIds.length > 0) {
    await Authorization.skip(() =>
      db.silent(() =>
        db.deleteDocuments(junctionCollection, [
          Query.equal(relationship.getId(), [document.getId()]),
          Query.equal(options.twoWayKey!, disconnectIds),
        ]),
      ),
    );
  }

  const targetIds = setIds !== undefined ? setIds : connectIds;
  const uniqueTargetIds = Array.from(new Set(targetIds)); // de-dupe but keep insertion order

  if (uniqueTargetIds.length > 0) {
    const relatedDocs = await db.silent(() =>
      db.find(options.relatedCollection, (qb) =>
        qb.equal("$id", ...uniqueTargetIds),
      ),
    );

    const foundIdsSet = new Set(relatedDocs.map((d) => d.getId()));
    const missingIds = uniqueTargetIds.filter((id) => !foundIdsSet.has(id));

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

    await db.silent(() =>
      Authorization.skip(() =>
        db.createDocuments(junctionCollection, linkDocs),
      ),
    );
  }
}

/**
 * Update relationships of a document.
 */
export async function updateDocumentRelationships(
  db: Database,
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
    if (db.relationStack.has(loopKey)) continue;
    db.relationStack.add(loopKey);

    try {
      if (
        type === RelationEnum.OneToOne ||
        (type === RelationEnum.OneToMany && side === RelationSideEnum.Child) ||
        (type === RelationEnum.ManyToOne && side === RelationSideEnum.Parent)
      ) {
        if (value !== null && typeof value !== "string") {
          throw new DatabaseException(
            "Invalid value for relationship: must be a string or",
          );
        }

        const relatedDoc = await db.silent(() =>
          db.getDocument(options.relatedCollection, value),
        );

        if (relatedDoc.empty() && !db.checksRelationshipsExist) {
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
            await db.silent(() =>
              Authorization.skip(() =>
                db.skipCheckRelationshipsExist(() =>
                  db.updateDocuments(
                    options.relatedCollection,
                    new Doc({ [options.twoWayKey!]: null }),
                    (qb) => qb.equal(options.twoWayKey!, document.getId()),
                  ),
                ),
              ),
            );

            // Set new relationship
            if (value !== null && typeof value === "string") {
              await db.silent(() =>
                Authorization.skip(() =>
                  db.skipCheckRelationshipsExist(() =>
                    db.updateDocument(
                      options.relatedCollection,
                      value,
                      new Doc({ [options.twoWayKey!]: document.getId() }),
                    ),
                  ),
                ),
              );
            }
          }
        }
      } else if (
        type === RelationEnum.ManyToMany ||
        (type === RelationEnum.OneToMany && side === RelationSideEnum.Parent) ||
        (type === RelationEnum.ManyToOne && side === RelationSideEnum.Child)
      ) {
        const { setIds, connectIds, disconnectIds } =
          formatRelationValue(value);
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
          await handleManyToMany(
            db,
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
            await db.silent(() =>
              Authorization.skip(() =>
                db.skipCheckRelationshipsExist(() =>
                  db.updateDocuments(
                    options.relatedCollection,
                    new Doc({ [options.twoWayKey!]: null }),
                    (qb) => qb.equal(options.twoWayKey!, document.getId()),
                  ),
                ),
              ),
            );

            //  If new set is not empty, set new children
            if (setIds && setIds.length > 0) {
              await db.silent(() =>
                Authorization.skip(() =>
                  db.skipCheckRelationshipsExist(() =>
                    db.updateDocuments(
                      options.relatedCollection,
                      new Doc({ [options.twoWayKey!]: document.getId() }),
                      [Query.equal("$id", setIds)],
                    ),
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
              await db.silent(() =>
                Authorization.skip(() =>
                  db.skipCheckRelationshipsExist(() =>
                    db.updateDocuments(
                      options.relatedCollection,
                      new Doc({ [options.twoWayKey!]: null }),
                      [Query.equal("$id", Array.from(disconnectSet))],
                    ),
                  ),
                ),
              );
            }

            // Connect
            if (connectSet.size > 0) {
              await db.silent(() =>
                Authorization.skip(() =>
                  db.skipCheckRelationshipsExist(() =>
                    db.updateDocuments(
                      options.relatedCollection,
                      new Doc({ [options.twoWayKey!]: document.getId() }),
                      [Query.equal("$id", Array.from(connectSet))],
                    ),
                  ),
                ),
              );
            }
          }
        }
      }
    } finally {
      db.relationStack.delete(loopKey);
    }
  }
  return document;
}

/**
 * Delete all relationships of a document.
 */
export async function deleteDocumentRelationships(
  db: Database,
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
    if (db.relationStack.has(loopKey)) continue;
    db.relationStack.add(loopKey);

    try {
      await handleOnDelete(db, collection, document, relationship, options);
    } finally {
      db.relationStack.delete(loopKey);
    }
  }
}

/**
 * Handle deletion of related documents based on the relationship options.
 * This method is called when a document is deleted and handles the cascading effects
 * according to the `onDelete` option specified in the relationship.
 */
export async function handleOnDelete(
  db: Database,
  collection: Doc<Collection>,
  document: Doc<Record<string, any>>,
  relationship: Doc<Attribute>,
  options: RelationOptions,
): Promise<void> {
  const adapter = db.getAdapter();
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
    const relatedCollection = await db.getCollection(
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
    const junctionCollection = adapter.getJunctionTable(
      parentColl.getSequence(),
      childColl.getSequence(),
      parentAttr,
      childAttr,
    );

    if (onDelete === OnDelete.Restrict) {
      const count = await Authorization.skip(() =>
        db.count(
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
        db.deleteDocuments(junctionCollection, [
          Query.equal(parentAttr, [document.getId()]),
        ]),
      );
    } else if (onDelete === OnDelete.Cascade) {
      // Internal maintenance: cascading is deterministic schema behavior
      // authorized by the parent delete. Skipping target auth keeps
      // cascades atomic instead of partially applied or blocked.
      await Authorization.skip(async () => {
        const relatedIds = (
          await db.find(junctionCollection, (qb) =>
            qb.equal(parentAttr, document.getId()),
          )
        ).map((doc) => doc.get(childAttr));

        await db.deleteDocuments(junctionCollection, [
          Query.equal(parentAttr, [document.getId()]),
        ]);

        relatedIds.length &&
          (await db.deleteDocuments(relatedCollection.getId(), (qb) =>
            qb.equal("$id", ...relatedIds),
          ));
      });
    }
    return;
  }

  // Non-ManyToMany
  if (!targetCollectionId || !targetField) return;

  if (onDelete === OnDelete.Restrict) {
    const count = await Authorization.skip(() =>
      db.count(
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
      db.updateDocuments(targetCollectionId, new Doc({ [targetField]: null }), [
        Query.equal(targetField, [document.getId()]),
      ]),
    );
  } else if (onDelete === OnDelete.Cascade) {
    // Internal maintenance: see Cascade comment above.
    await Authorization.skip(() =>
      db.deleteDocuments(targetCollectionId, (qb) =>
        qb.equal(targetField, document.getId()),
      ),
    );
  }
}
