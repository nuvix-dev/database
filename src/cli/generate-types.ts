#!/usr/bin/env bun

import { writeFileSync, mkdirSync, existsSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { parseArgs } from "node:util";
import { ConfigLoader } from "../config/loader.js";
import { generateTypes } from "../utils/generate-types.js";
import { NuvixDBConfig, CLIOptions } from "../config/types.js";

class TypeGeneratorCLI {
  private async run(): Promise<void> {
    try {
      const cliOptions = this.parseArguments();

      if (cliOptions.help) {
        this.showHelp();
        return;
      }

      if (cliOptions.init) {
        this.initConfig();
        return;
      }

      const config = await ConfigLoader.loadConfig(cliOptions);
      ConfigLoader.validateConfig(config);

      if (cliOptions.verbose) {
        console.log(
          "📋 Configuration loaded:",
          JSON.stringify(config, null, 2),
        );
      }

      const generatedTypes = this.generateTypesFromConfig(config);

      if (cliOptions.dryRun) {
        console.log("🔍 Dry run - Generated types:");
        console.log("=".repeat(80));
        console.log(generatedTypes);
        console.log("=".repeat(80));
        return;
      }

      this.writeTypesToFile(generatedTypes, config, cliOptions);

      console.log("✅ Types generated successfully!");
      console.log(`📁 Output: ${config.typeGeneration?.outputPath}`);

      if (cliOptions.watch) {
        this.watchForChanges(config, cliOptions);
      }
    } catch (error) {
      console.error(
        "❌ Error:",
        error instanceof Error ? error.message : error,
      );
      process.exit(1);
    }
  }

  private parseArguments(): CLIOptions & { help?: boolean; init?: boolean } {
    const { values } = parseArgs({
      args: process.argv.slice(2),
      options: {
        config: {
          type: "string",
          short: "c",
          description: "Path to configuration file",
        },
        output: {
          type: "string",
          short: "o",
          description: "Output path for generated types",
        },
        watch: {
          type: "boolean",
          short: "w",
          description: "Watch for changes and regenerate",
        },
        verbose: {
          type: "boolean",
          short: "v",
          description: "Verbose output",
        },
        "dry-run": {
          type: "boolean",
          short: "d",
          description: "Show output without writing files",
        },
        force: {
          type: "boolean",
          short: "f",
          description: "Force overwrite existing files",
        },
        help: {
          type: "boolean",
          short: "h",
          description: "Show help",
        },
        init: {
          type: "boolean",
          description: "Initialize a new configuration file",
        },
      },
      allowPositionals: false,
    });

    return {
      config: values.config,
      output: values.output,
      watch: values.watch,
      verbose: values.verbose,
      dryRun: values["dry-run"],
      force: values.force,
      help: values.help,
      init: values.init,
    };
  }

  private showHelp(): void {
    console.log(`
🚀 Nuvix DB Type Generator

USAGE:
  nuvix-types [OPTIONS]

OPTIONS:
  -c, --config <path>     Path to configuration file (default: auto-detect)
  -o, --output <path>     Output path for generated types
  -w, --watch             Watch for changes and regenerate
  -v, --verbose           Verbose output
  -d, --dry-run           Show output without writing files
  -f, --force             Force overwrite existing files
  -h, --help              Show this help message
      --init              Initialize a new configuration file

EXAMPLES:
  nuvix-types                                    # Use auto-detected config
  nuvix-types -c ./my-config.ts                 # Use specific config
  nuvix-types -o ./types/db.ts                  # Override output path
  nuvix-types --watch --verbose                 # Watch mode with verbose output
  nuvix-types --init                            # Create example config file
  nuvix-types --dry-run                         # Preview generated types

CONFIGURATION:
  The CLI looks for configuration files in this order:
  - nuvix-db.config.ts
  - nuvix-db.config.js
  - nuvix-db.config.mjs
`);
  }

  private initConfig(): void {
    const configPath = resolve(process.cwd(), "nuvix-db.config.ts");

    if (existsSync(configPath)) {
      console.log("❌ Configuration file already exists:", configPath);
      console.log("💡 Use --force to overwrite or choose a different name");
      return;
    }

    const exampleConfig = ConfigLoader.createExampleConfig();
    writeFileSync(configPath, exampleConfig, "utf8");

    console.log("✅ Configuration file created:", configPath);
    console.log(
      "📝 Edit the file to define your collections and run the generator again",
    );
  }

  private generateTypesFromConfig(config: NuvixDBConfig): string {
    const { collections, typeGeneration } = config;

    let generatedTypes = generateTypes(collections, {
      includeImports: typeGeneration?.includeImports,
      includeDocTypes: typeGeneration?.includeDocTypes,
      includeEntityMap: typeGeneration?.includeEntityMap,
      generateUtilityTypes: typeGeneration?.generateUtilityTypes,
      generateQueryTypes: typeGeneration?.generateQueryTypes,
      generateInputTypes: typeGeneration?.generateInputTypes,
      generateValidationTypes: typeGeneration?.generateValidationTypes,
      includeMetaDataTypes: typeGeneration?.includeMetaDataTypes,
      packageName: typeGeneration?.packageName,
    });

    // Add file header
    if (typeGeneration?.fileHeader) {
      generatedTypes = `${typeGeneration.fileHeader}\n${generatedTypes}`;
    }

    // Add custom types
    if (typeGeneration?.customTypes) {
      generatedTypes = `${generatedTypes}\n\n${typeGeneration.customTypes}`;
    }

    return generatedTypes;
  }

  private writeTypesToFile(
    generatedTypes: string,
    config: NuvixDBConfig,
    cliOptions: CLIOptions,
  ): void {
    const outputPath = resolve(
      config.typeGeneration?.outputPath || "./generated-types.ts",
    );

    // Check if file exists and force flag is not set
    if (existsSync(outputPath) && !cliOptions.force) {
      console.log("⚠️  Output file already exists:", outputPath);
      console.log(
        "💡 Use --force to overwrite or choose a different output path",
      );
      return;
    }

    // Create directory if it doesn't exist
    const dir = dirname(outputPath);
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
      console.log("📁 Created directory:", dir);
    }

    // Write the file
    writeFileSync(outputPath, generatedTypes, "utf8");
  }

  private watchForChanges(config: NuvixDBConfig, cliOptions: CLIOptions): void {
    console.log("👀 Watching for changes... (Press Ctrl+C to stop)");

    // Simple implementation - in a real scenario you'd want to use a proper file watcher
    // like chokidar to watch the config file and any referenced files
    console.log("📝 Note: Watch mode is a basic implementation");
    console.log("🔄 Manually run the command again after making changes");
  }
}

// Run the CLI
const cli = new TypeGeneratorCLI();
cli["run"]().catch(console.error);
