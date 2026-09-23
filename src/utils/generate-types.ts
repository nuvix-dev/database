import { AttributeEnum } from "@core/enums.js";
import { Collection, RelationOptions, Attribute } from "@validators/schema.js";

const typeMap: Record<AttributeEnum, string> = {
  [AttributeEnum.String]: "string",
  [AttributeEnum.Integer]: "number",
  [AttributeEnum.Float]: "number",
  [AttributeEnum.Boolean]: "boolean",
  [AttributeEnum.Timestamptz]: "Date",
  [AttributeEnum.Json]: "Record<string, unknown>",
  [AttributeEnum.Relationship]: "string", // will be replaced dynamically
  [AttributeEnum.Virtual]: "never",
  [AttributeEnum.Uuid]: "string",
};

export interface TypeGenerationOptions {
  includeImports?: boolean;
  includeDocTypes?: boolean;
  includeEntityMap?: boolean;
  packageName?: string;
  generateUtilityTypes?: boolean;
  generateQueryTypes?: boolean;
  generateInputTypes?: boolean;
  generateValidationTypes?: boolean;
  includeMetaDataTypes?: boolean;
  filterTypes?: Record<string, string>;
}

/**
 * Base entity interface emitted alongside imports so generated files are
 * self-describing. Mirrors the runtime `IEntity` from src/types.ts.
 */
const IENTITY_DEFINITION = `export interface IEntity {
  $id: string;
  $createdAt: Date;
  $updatedAt: Date;
  $permissions: string[];
  $sequence: number;
  $collection: string;
  $tenant?: number | null; // Optional tenant ID for multi-tenant support
  $schema?: string; // Optional schema where the entity is stored
}`;

export function generateTypes(
  collections: Collection[],
  options: TypeGenerationOptions = {},
): string {
  const {
    includeImports = true,
    includeDocTypes = true,
    includeEntityMap = true,
    generateUtilityTypes = true,
    generateQueryTypes = false,
    generateInputTypes = true,
    generateValidationTypes = false,
    includeMetaDataTypes = false,
    packageName = "@nuvix/db",
    filterTypes = {},
  } = options;

  const parts: string[] = [];

  // Import statements
  // Note: IEntity is intentionally NOT imported here — a local definition is
  // emitted below so generated files are self-describing (importing it as well
  // would conflict with the local declaration).
  if (includeImports) {
    const imports = `import type { Doc } from "${packageName}";`;
    parts.push(imports);
    parts.push(IENTITY_DEFINITION);
  }

  // Individual entity interfaces
  const entityInterfaces = collections.map((col) => {
    return generateEntityInterface(col, collections, filterTypes);
  });
  parts.push(...entityInterfaces);

  // Doc type aliases for each collection
  if (includeDocTypes && collections.length > 0) {
    const docTypes = collections.map((col) => {
      const interfaceName = pascalCase(col.name);
      const docTypeName = `${interfaceName}Doc`;
      return `export type ${docTypeName} = Doc<${interfaceName}>;`;
    });

    if (docTypes.length > 0) {
      parts.push(`// Document Types\n${docTypes.join("\n")}`);
    }
  }

  // Utility types
  if (generateUtilityTypes && collections.length > 0) {
    const utilityTypes = generateUtilityTypesInternal(collections);
    if (utilityTypes) {
      parts.push(utilityTypes);
    }
  }

  // Query types
  if (generateQueryTypes && collections.length > 0) {
    const queryTypes = generateQueryTypesInternal(collections);
    if (queryTypes) {
      parts.push(queryTypes);
    }
  }

  // Input types
  if (generateInputTypes && collections.length > 0) {
    const inputTypes = generateInputTypesInternal(collections);
    if (inputTypes) {
      parts.push(inputTypes);
    }
  }

  // Validation types
  if (generateValidationTypes && collections.length > 0) {
    const validationTypes = generateValidationTypesInternal(collections);
    if (validationTypes) {
      parts.push(validationTypes);
    }
  }

  // Entity map interface
  if (includeEntityMap) {
    const entityMap = generateEntityMap(collections, packageName);
    parts.push(entityMap);
  }

  // Collection metadata
  if (includeMetaDataTypes && collections.length > 0) {
    const collectionMeta = generateCollectionMetadata(collections);
    if (collectionMeta) {
      parts.push(collectionMeta);
    }
  }

  const result = parts.join("\n\n");
  return result;
}

function generateEntityInterface(
  collection: Collection,
  allCollections: Collection[],
  filterTypes: Record<string, string> = {},
): string {
  const interfaceName = pascalCase(collection.name);

  const attributes = collection.attributes
    .map((attr) => generateAttributeType(attr, allCollections, filterTypes))
    .join("\n");

  return `export interface ${interfaceName} extends IEntity {\n${attributes}\n}`;
}

function generateAttributeType(
  attr: Attribute,
  allCollections: Collection[],
  filterTypes: Record<string, string> = {},
): string {
  let tsType: string;

  // 1. Explicit __type on the attribute takes highest priority
  if (attr.__type) {
    tsType = attr.__type;
  }
  // 2. First filter type if registered in config
  else if (
    attr.filters &&
    attr.filters.length > 0 &&
    filterTypes[attr.filters[0]!]
  ) {
    tsType = filterTypes[attr.filters[0]!]!;
  }
  // 3. Relationship types
  else if (attr.type === AttributeEnum.Relationship) {
    tsType = generateRelationshipType(attr, allCollections);
  }
  // 4. Enum literal types
  else if (attr.format === "enum" && attr.formatOptions?.["values"]) {
    tsType = generateEnumType(attr);
  }
  // 5. Default/basic types
  else {
    tsType = generateBasicType(attr);
  }

  // Handle array types
  if (attr.array) {
    if (!tsType.endsWith("[]") && !tsType.startsWith("Array<")) {
      tsType =
        tsType.includes("|") || tsType.includes("&")
          ? `(${tsType})[]`
          : `${tsType}[]`;
    }
  }

  // Handle optional fields
  const optional = attr.required ? "" : "?";
  if (
    optional &&
    attr.default !== undefined &&
    attr.default !== null &&
    !(
      typeof attr.default === "object" && Object.keys(attr.default).length === 0
    )
  ) {
    tsType += ` | ${JSON.stringify(attr.default)}`;
  }

  // Add JSDoc comments for better type information
  const comment = generateAttributeComment(attr);

  return `${comment}    ${attr.key}${optional}: ${tsType};`;
}

function generateRelationshipType(
  attr: Attribute,
  allCollections: Collection[],
): string {
  const opts = attr.options as RelationOptions;
  const relatedCollection = allCollections.find(
    (c) => c.$id === opts?.relatedCollection,
  );

  if (relatedCollection) {
    const relatedInterfaceName = pascalCase(relatedCollection.name);
    return `${relatedInterfaceName}['$id'] | ${relatedInterfaceName}`;
  }

  return "string";
}

function generateEnumType(attr: Attribute): string {
  const values = (attr.formatOptions?.["values"] as string[]) || [];
  return values.map((v) => JSON.stringify(v)).join(" | ");
}

function generateBasicType(attr: Attribute): string {
  if (attr.__type) {
    return attr.__type;
  }
  return typeMap[attr.type as AttributeEnum] ?? "any";
}

function generateAttributeComment(attr: Attribute): string {
  const comments: string[] = [];

  // Add format information as comments
  if (attr.format) {
    comments.push(`@format ${attr.format}`);
  }

  if (attr.type === AttributeEnum.Relationship) {
    const opts = attr.options as RelationOptions;
    if (opts && opts.relatedCollection) {
      comments.push(`@relationship ${opts.relatedCollection}`);
    }
  }

  if (attr.filters && attr.filters.length > 0) {
    comments.push(
      attr.filters.length === 1
        ? `@filter ${attr.filters[0]}`
        : `@filters ${attr.filters.join(", ")}`,
    );
  }

  if (attr.formatOptions) {
    if (attr.formatOptions["min"] !== undefined) {
      comments.push(`@min ${attr.formatOptions["min"]}`);
    }
    if (attr.formatOptions["max"] !== undefined) {
      comments.push(`@max ${attr.formatOptions["max"]}`);
    }
    if (attr.formatOptions["pattern"]) {
      comments.push(`@pattern ${attr.formatOptions["pattern"]}`);
    }
    if (attr.formatOptions["minLength"] !== undefined) {
      comments.push(`@minLength ${attr.formatOptions["minLength"]}`);
    }
    if (attr.formatOptions["maxLength"] !== undefined) {
      comments.push(`@maxLength ${attr.formatOptions["maxLength"]}`);
    }
  }

  if (attr.array) {
    comments.push(`@array`);
  }

  if (!attr.required) {
    comments.push(`@optional`);
  } else {
    comments.push(`@required`);
  }

  if (attr.default !== undefined && attr.default !== null) {
    comments.push(`@default ${JSON.stringify(attr.default)}`);
  }

  if (comments.length === 0) {
    return "";
  }

  if (comments.length === 1) {
    return `    /** ${comments[0]} */\n`;
  }

  return `    /**\n${comments.map((c) => `     * ${c}`).join("\n")}\n     */\n`;
}

function generateEntityMap(
  collections: Collection[],
  packageName: string,
): string {
  const entityMapEntries = collections
    .map((col) => {
      const interfaceName = pascalCase(col.name);
      return `  "${col.$id}": ${interfaceName};`;
    })
    .join("\n");

  return `export interface Entities {\n${entityMapEntries}\n}

type GeneratedEntitiesRegistry = Entities;

/**
 * Opt-in integration: including or importing this generated file augments
 * the package registry. Session APIs then infer collection IDs, documents,
 * and query attributes while their arbitrary-string fallback remains intact.
 */
declare module ${JSON.stringify(packageName)} {
  interface Entities extends GeneratedEntitiesRegistry {}
}`;
}

function pascalCase(str: string): string {
  return str
    .split(/[^a-zA-Z0-9]/)
    .filter(Boolean)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join("");
}

// Utility function to generate individual entity type (for selective generation)
export function generateEntityType(
  collection: Collection,
  allCollections: Collection[],
  options: { filterTypes?: Record<string, string> } = {},
): string {
  return generateEntityInterface(
    collection,
    allCollections,
    options.filterTypes,
  );
}

// Utility function to generate Doc type for a specific collection
export function generateDocType(
  collection: Collection,
  packageName: string = "@nuvix/db",
): string {
  const interfaceName = pascalCase(collection.name);
  const docTypeName = `${interfaceName}Doc`;
  return `export type ${docTypeName} = Doc<${interfaceName}>;`;
}

function generateUtilityTypesInternal(collections: Collection[]): string {
  if (collections.length === 0) return "";

  const utilityTypes = collections
    .map((col) => {
      const interfaceName = pascalCase(col.name);
      return `
// Utility types for ${interfaceName}
export type ${interfaceName}Create = Omit<${interfaceName}, '$id' | '$createdAt' | '$updatedAt' | '$permissions' | '$sequence' | '$collection' | '$tenant' | '$schema'>;
export type ${interfaceName}Update = Partial<${interfaceName}Create>;
export type ${interfaceName}Keys = keyof ${interfaceName};
export type ${interfaceName}Values = ${interfaceName}[${interfaceName}Keys];`;
    })
    .join("\n");

  return `// Utility Types\n${utilityTypes}`;
}

function generateQueryTypesInternal(collections: Collection[]): string {
  if (collections.length === 0) return "";

  const queryTypes = collections
    .map((col) => {
      const interfaceName = pascalCase(col.name);
      return `
// Query types for ${interfaceName}
export type ${interfaceName}Query = {
  [K in keyof ${interfaceName}]?: ${interfaceName}[K] | { $in?: ${interfaceName}[K][] } | { $ne?: ${interfaceName}[K] } | { $exists?: boolean } | { $gt?: ${interfaceName}[K] } | { $gte?: ${interfaceName}[K] } | { $lt?: ${interfaceName}[K] } | { $lte?: ${interfaceName}[K] } | { $regex?: string } | { $contains?: string };
} & {
  $or?: ${interfaceName}Query[];
  $and?: ${interfaceName}Query[];
  $limit?: number;
  $offset?: number;
  $orderBy?: { [K in keyof ${interfaceName}]?: 'asc' | 'desc' };
};`;
    })
    .join("\n");

  return `// Query Types\n${queryTypes}`;
}

function generateInputTypesInternal(collections: Collection[]): string {
  if (collections.length === 0) return "";

  const inputTypes = collections
    .map((col) => {
      const interfaceName = pascalCase(col.name);
      return `
// Input types for ${interfaceName}
export type ${interfaceName}Input = ${interfaceName}Create;
export type ${interfaceName}CreateInput = ${interfaceName}Create;
export type ${interfaceName}UpdateInput = ${interfaceName}Update;`;
    })
    .join("\n");

  return `// Input Types\n${inputTypes}`;
}

function generateValidationTypesInternal(collections: Collection[]): string {
  if (collections.length === 0) return "";

  const validationTypes = collections
    .map((col) => {
      const interfaceName = pascalCase(col.name);
      const requiredFields = col.attributes
        .filter((attr) => attr.required)
        .map((attr) => `'${attr.key}'`)
        .join(" | ");

      const optionalFields = col.attributes
        .filter((attr) => !attr.required)
        .map((attr) => `'${attr.key}'`)
        .join(" | ");

      return `
// Validation types for ${interfaceName}
export type ${interfaceName}RequiredFields = ${requiredFields || "never"};
export type ${interfaceName}OptionalFields = ${optionalFields || "never"};
export type ${interfaceName}ValidationResult = {
  isValid: boolean;
  errors: { field: string; message: string }[];
};`;
    })
    .join("\n");

  return `// Validation Types\n${validationTypes}`;
}

function generateCollectionMetadata(collections: Collection[]): string {
  if (collections.length === 0) return "";

  const metadata = collections
    .map((col) => {
      const interfaceName = pascalCase(col.name);
      const attributes = col.attributes.map((attr) => ({
        key: attr.key,
        type: attr.type,
        required: attr.required || false,
        array: attr.array || false,
        format: attr.format || null,
      }));

      return `
// Metadata for ${interfaceName}
export const ${interfaceName}Metadata = {
  $id: "${col.$id}",
  name: "${col.name}",
  collectionName: "${col.$collection}",
  attributes: ${JSON.stringify(attributes, null, 2)},
  indexes: ${JSON.stringify(col.indexes || [], null, 2)},
  documentSecurity: ${col.documentSecurity || false}
} as const;`;
    })
    .join("\n");

  const allCollectionsMetadata = `
// All Collections Metadata
export const AllCollectionsMetadata = {
${collections.map((col) => `  "${col.$id}": ${pascalCase(col.name)}Metadata`).join(",\n")}
} as const;

export type CollectionId = keyof typeof AllCollectionsMetadata;`;

  return `// Collection Metadata\n${metadata}\n${allCollectionsMetadata}`;
}
