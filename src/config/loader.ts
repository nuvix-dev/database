import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { NuvixDBConfig, DEFAULT_CONFIG, CLIOptions } from "./types.js";

export class ConfigLoader {
  private static readonly CONFIG_NAMES = [
    "nuvix-db.config.ts",
    "nuvix-db.config.js",
    "nuvix-db.config.mjs",
  ];

  static async loadConfig(options: CLIOptions = {}): Promise<NuvixDBConfig> {
    const configPath = options.config || this.findConfigFile();

    if (!configPath) {
      console.warn("No configuration file found. Using default configuration.");
      return this.mergeWithDefaults({});
    }

    if (!existsSync(configPath)) {
      throw new Error(`Configuration file not found: ${configPath}`);
    }

    try {
      const config = await this.importConfig(configPath);
      return this.mergeWithDefaults(config, options);
    } catch (error) {
      throw new Error(
        `Failed to load configuration from ${configPath}: ${error}`,
      );
    }
  }

  private static findConfigFile(): string | null {
    const cwd = process.cwd();

    for (const configName of this.CONFIG_NAMES) {
      const configPath = resolve(cwd, configName);
      if (existsSync(configPath)) {
        return configPath;
      }
    }

    return null;
  }

  private static async importConfig(
    configPath: string,
  ): Promise<Partial<NuvixDBConfig>> {
    const absolutePath = resolve(configPath);

    try {
      // For ES modules
      const module = await import(`file://${absolutePath}`);
      return module.default || module;
    } catch (error) {
      // Fallback for CommonJS
      try {
        const module = require(absolutePath);
        return module.default || module;
      } catch (requireError) {
        throw new Error(`Failed to import config: ${error}`);
      }
    }
  }

  private static mergeWithDefaults(
    userConfig: Partial<NuvixDBConfig>,
    cliOptions: CLIOptions = {},
  ): NuvixDBConfig {
    const config: NuvixDBConfig = {
      collections: userConfig.collections || [],
      typeGeneration: {
        ...DEFAULT_CONFIG.typeGeneration,
        ...userConfig.typeGeneration,
      },
      database: {
        ...userConfig.database,
      },
      options: {
        ...DEFAULT_CONFIG.options,
        ...userConfig.options,
      },
    };

    // Apply CLI overrides
    if (cliOptions.output) {
      config.typeGeneration!.outputPath = cliOptions.output;
    }

    if (cliOptions.verbose) {
      config.options!.debug = true;
    }

    return config;
  }

  static validateConfig(config: NuvixDBConfig): void {
    if (!config.collections || config.collections.length === 0) {
      throw new Error("Configuration must include at least one collection");
    }

    for (const collection of config.collections) {
      if (!collection.$id || !collection.name || !collection.$collection) {
        throw new Error(
          `Invalid collection configuration: ${JSON.stringify(collection)}`,
        );
      }

      if (!collection.attributes || collection.attributes.length === 0) {
        throw new Error(
          `Collection '${collection.name}' must have at least one attribute`,
        );
      }

      for (const attribute of collection.attributes) {
        if (!attribute.$id || !attribute.key || !attribute.type) {
          throw new Error(
            `Invalid attribute in collection '${collection.name}': ${JSON.stringify(attribute)}`,
          );
        }
      }
    }

    if (
      config.typeGeneration?.outputPath &&
      !config.typeGeneration.outputPath.endsWith(".ts")
    ) {
      throw new Error("Output path must be a TypeScript file (.ts extension)");
    }
  }

  static createExampleConfig(): string {
    return `import { NuvixDBConfig } from '@nuvix/db';
import { AttributeType } from '@nuvix/db';

const config: NuvixDBConfig = {
  collections: [
    {
      $id: 'users',
      name: 'users',
      $collection: 'users',
      attributes: [
        {
          $id: 'name',
          key: 'name',
          type: AttributeType.String,
          required: true,
          array: false,
        },
        {
          $id: 'email',
          key: 'email',
          type: AttributeType.String,
          required: true,
          array: false,
          format: 'email',
        },
        {
          $id: 'age',
          key: 'age',
          type: AttributeType.Integer,
          required: false,
          array: false,
        },
      ],
    },
    {
      $id: 'posts',
      name: 'posts',
      $collection: 'posts',
      attributes: [
        {
          $id: 'title',
          key: 'title',
          type: AttributeType.String,
          required: true,
          array: false,
        },
        {
          $id: 'content',
          key: 'content',
          type: AttributeType.String,
          required: true,
          array: false,
        },
        {
          $id: 'author',
          key: 'author',
          type: AttributeType.Relationship,
          required: true,
          array: false,
          options: {
            relatedCollection: 'users',
            relationType: 'many-to-one',
            side: 'child',
            onDelete: 'CASCADE',
          },
        },
      ],
    },
  ],
  
  typeGeneration: {
    outputPath: './src/types/generated.ts',
    packageName: '@nuvix/db',
    includeDocTypes: true,
    generateUtilityTypes: true,
    generateQueryTypes: true,
    generateInputTypes: true,
  },
  
  options: {
    debug: false,
    strict: true,
  },
};

export default config;
`;
  }
}
