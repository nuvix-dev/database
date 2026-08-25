/**
 * Pure validation helpers extracted from `Base` (src/core/base.ts).
 *
 * These functions validate user-supplied values against schema constraints
 * (attribute default values, required filters, relationship mutation payloads).
 * They are intentionally stateless — no adapter, cache, or instance access —
 * so they can be unit-tested in isolation. The `Base` class keeps thin
 * delegate methods with unchanged signatures for facade compatibility.
 */

import { AttributeEnum } from "./enums.js";
import { DatabaseException, RelationshipException } from "@errors/index.js";

/**
 * Validates that a default value matches the given attribute type.
 * Arrays are validated element-by-element. Throws `DatabaseException` on mismatch.
 */
export function validateDefaultTypes(
  type: AttributeEnum,
  value: unknown,
): void {
  if (value === null || value === undefined) {
    // Disable null. No validation required
    return;
  }

  if (Array.isArray(value)) {
    for (const v of value) {
      validateDefaultTypes(type, v);
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

/**
 * Returns the filters that attributes of the given type are required to declare.
 */
export function getRequiredFilters(type: AttributeEnum): string[] {
  switch (type) {
    default:
      return [];
  }
}

/**
 * Parses and validates a relationship mutation payload (`connect` / `disconnect`
 * / `set`) into disjoint ID lists. Throws `RelationshipException` on invalid input.
 */
export function formatRelationValue(value: any): {
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
