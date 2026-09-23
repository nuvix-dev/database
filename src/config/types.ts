import type { Collection } from "@validators/schema.js";

export interface NuvixDBConfig {
  /** Database collections */
  collections: Collection[];

  /** Filter types mapping (filter name -> TypeScript type) */
  filterTypes?: Record<string, string>;

  /** Type generation options */
  typeGeneration?: {
    /** Output path for generated types */
    outputPath?: string;

    /** Package name for imports */
    packageName?: string;

    /** Include import statements */
    includeImports?: boolean;

    /** Include Doc type aliases (e.g., UserDoc = Doc<User>) */
    includeDocTypes?: boolean;

    /** Include entity map interface */
    includeEntityMap?: boolean;

    /** Generate utility types (Create, Update, etc.) */
    generateUtilityTypes?: boolean;

    /** Generate query types for filtering */
    generateQueryTypes?: boolean;

    /** Generate input types for operations */
    generateInputTypes?: boolean;

    /** Generate validation types */
    generateValidationTypes?: boolean;

    includeMetaDataTypes?: boolean;

    /** Filter types mapping (filter name -> TypeScript type) */
    filterTypes?: Record<string, string>;

    /** Custom file header */
    fileHeader?: string;

    /** Additional custom types to append */
    customTypes?: string;
  };

  /** Database connection options */
  database?: {
    host?: string;
    port?: number;
    database?: string;
    username?: string;
    password?: string;
  };

  /** Additional configuration options */
  options?: {
    /** Enable debugging */
    debug?: boolean;

    /** Validation strictness */
    strict?: boolean;

    /** Custom naming conventions */
    naming?: {
      /** Collection naming convention */
      collections?: "camelCase" | "snake_case" | "kebab-case" | "PascalCase";

      /** Attribute naming convention */
      attributes?: "camelCase" | "snake_case" | "kebab-case" | "PascalCase";
    };
  };
}

export interface CLIOptions {
  /** Path to config file */
  config?: string;

  /** Output path override */
  output?: string;

  /** Watch for changes */
  watch?: boolean;

  /** Verbose logging */
  verbose?: boolean;

  /** Dry run (don't write files) */
  dryRun?: boolean;

  /** Force overwrite existing files */
  force?: boolean;
}

export const DEFAULT_CONFIG: Partial<NuvixDBConfig> = {
  typeGeneration: {
    outputPath: "./src/types/generated.ts",
    packageName: "@nuvix/db",
    includeImports: true,
    includeDocTypes: true,
    includeEntityMap: true,
    generateUtilityTypes: true,
    generateQueryTypes: false,
    generateInputTypes: true,
    generateValidationTypes: false,
    filterTypes: {},
    fileHeader: `// This file is auto-generated. Do not edit manually.\n// Generated on: ${new Date().toISOString()}\n`,
  },
  filterTypes: {},
  options: {
    debug: false,
    strict: true,
    naming: {
      collections: "camelCase",
      attributes: "camelCase",
    },
  },
};
