import { AttributeEnum } from "@core/enums.js";
import { Collection, RelationOptions, Attribute } from "@validators/schema.js";

const typeMap: Record<AttributeEnum, string> = {
  [AttributeEnum.String]: "string",
  [AttributeEnum.Integer]: "number",
  [AttributeEnum.Float]: "number",
  [AttributeEnum.Boolean]: "boolean",
  [AttributeEnum.Timestamptz]: "string | Date",
  [AttributeEnum.Json]: "Record<string, any>",
  [AttributeEnum.Relationship]: "string", // will be replaced dynamically
  [AttributeEnum.Virtual]: "never",
  [AttributeEnum.Uuid]: "string",
};

interface TypeGenerationOptions {
  includeImports?: boolean;
  includeDocTypes?: boolean;
  includeEntityMap?: boolean;
  packageName?: string;
  generateUtilityTypes?: boolean;
  generateQueryTypes?: boolean;
  generateInputTypes?: boolean;
  generateValidationTypes?: boolean;
  includeMetaDataTypes?: boolean;
}

export function generateTypes(
  collections: Collection[],
  options: TypeGenerationOptions = {},
): string {
  const {
    includeImports = true,
    includeDocTypes = true,
    includeEntityMap = true,
    generateUtilityTypes = true,
    generateQueryTypes = true,
    generateInputTypes = true,
    generateValidationTypes = false,
    includeMetaDataTypes = false,
    packageName = "@nuvix/db",
  } = options;

  const parts: string[] = [];

  // Import statements
  if (includeImports) {
    const imports = `import { Doc, IEntity } from "${packageName}";`;
    parts.push(imports);
  }

  // Individual entity interfaces
  const entityInterfaces = collections.map((col) => {
    return generateEntityInterface(col, collections);
  });
  parts.push(...entityInterfaces);

  // Doc type aliases for each collection
  if (includeDocTypes) {
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
  if (generateUtilityTypes) {
    const utilityTypes = generateUtilityTypesInternal(collections);
    if (utilityTypes) {
      parts.push(utilityTypes);
    }
  }

  // Query types
  if (generateQueryTypes) {
    const queryTypes = generateQueryTypesInternal(collections);
    if (queryTypes) {
      parts.push(queryTypes);
    }
  }

  // Input types
  if (generateInputTypes) {
    const inputTypes = generateInputTypesInternal(collections);
    if (inputTypes) {
      parts.push(inputTypes);
    }
  }

  // Validation types
  if (generateValidationTypes) {
    const validationTypes = generateValidationTypesInternal(collections);
    if (validationTypes) {
      parts.push(validationTypes);
    }
  }

  // Entity map interface
  if (includeEntityMap) {
    const entityMap = generateEntityMap(collections);
    parts.push(entityMap);
  }

  // Collection metadata
  if (includeMetaDataTypes) {
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
): string {
  const interfaceName = pascalCase(collection.name);

  const attributes = collection.attributes
    .map((attr) => generateAttributeType(attr, allCollections))
    .join("\n");

  return `export interface ${interfaceName} extends IEntity {\n${attributes}\n}`;
}

function generateAttributeType(
  attr: Attribute,
  allCollections: Collection[],
): string {
  let tsType: string;

  // Handle relationship types
  if (attr.type === AttributeEnum.Relationship) {
    tsType = generateRelationshipType(attr, allCollections);
  }
  // Handle enum literal types
  else if (attr.format === "enum" && attr.formatOptions?.["values"]) {
    tsType = generateEnumType(attr);
  }
  // Handle other format options (like min/max for numbers, patterns for strings)
  else {
    tsType = generateBasicType(attr);
  }

  // Handle array types
  if (attr.array) {
    tsType += "[]";
  }

  // Handle optional fields
  const optional = attr.required ? "" : "?";

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

    // Check if it's a two-way relationship or has specific relation type
    if (opts?.twoWay) {
      // For two-way relationships, we might want to reference the full entity or just the ID
      return `${relatedInterfaceName}['$id'] | ${relatedInterfaceName}`;
    }

    // For regular relationships, allow both ID reference and full entity
    // This provides flexibility for populated vs non-populated relationships
    return `${relatedInterfaceName}['$id'] | ${relatedInterfaceName}`;
  }

  return "string";
}

function generateEnumType(attr: Attribute): string {
  const values = (attr.formatOptions?.["values"] as string[]) || [];
  return values.map((v) => JSON.stringify(v)).join(" | ");
}

function generateBasicType(attr: Attribute): string {
  let baseType = typeMap[attr.type as AttributeEnum] ?? "any";

  if (
    (attr.type === AttributeEnum.Json || attr.type === AttributeEnum.Virtual) &&
    attr.__type
  ) {
    baseType = attr.__type;
  }

  // Add more specific types based on format options
  if (attr.type === AttributeEnum.String && attr.formatOptions) {
    if (attr.formatOptions["pattern"]) {
      // Could add template literal types for patterns in the future
      baseType = "string";
    }
    if (attr.formatOptions["minLength"] || attr.formatOptions["maxLength"]) {
      // Could add branded types for length validation in the future
      baseType = "string";
    }
  }

  if (
    (attr.type === AttributeEnum.Integer ||
      attr.type === AttributeEnum.Float) &&
    attr.formatOptions
  ) {
    if (
      attr.formatOptions["min"] !== undefined ||
      attr.formatOptions["max"] !== undefined
    ) {
      // Could add branded types for range validation in the future
      baseType = "number";
    }
  }

  return baseType;
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
  }

  if (attr.default !== undefined) {
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

function generateEntityMap(collections: Collection[]): string {
  const entityMapEntries = collections
    .map((col) => {
      const interfaceName = pascalCase(col.name);
      return `  "${col.$id}": ${interfaceName};`;
    })
    .join("\n");

  return `export interface Entities {\n${entityMapEntries}\n}`;
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
): string {
  return generateEntityInterface(collection, allCollections);
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
  const utilityTypes = collections
    .map((col) => {
      const interfaceName = pascalCase(col.name);
      return `
// Utility types for ${interfaceName}
export type ${interfaceName}Create = Omit<${interfaceName}, '$id' | '$createdAt' | '$updatedAt' | '$sequence'>;
export type ${interfaceName}Update = Partial<${interfaceName}Create>;
export type ${interfaceName}Keys = keyof ${interfaceName};
export type ${interfaceName}Values = ${interfaceName}[${interfaceName}Keys];
export type ${interfaceName}Pick<K extends keyof ${interfaceName}> = Pick<${interfaceName}, K>;
export type ${interfaceName}Omit<K extends keyof ${interfaceName}> = Omit<${interfaceName}, K>;`;
    })
    .join("\n");

  return `// Utility Types\n${utilityTypes}`;
}

function generateQueryTypesInternal(collections: Collection[]): string {
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
  const inputTypes = collections
    .map((col) => {
      const interfaceName = pascalCase(col.name);
      return `
// Input types for ${interfaceName}
export type ${interfaceName}Input = Omit<${interfaceName}, '$id' | '$createdAt' | '$updatedAt' | '$permissions' | '$sequence' | '$collection' | '$tenant'>;
export type ${interfaceName}CreateInput = ${interfaceName}Input;
export type ${interfaceName}UpdateInput = Partial<${interfaceName}Input>;`;
    })
    .join("\n");

  return `// Input Types\n${inputTypes}`;
}

function generateValidationTypesInternal(collections: Collection[]): string {
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
