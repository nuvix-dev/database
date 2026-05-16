/**
 * Build script using Bun's native bundler.
 * Produces ESM + CJS outputs with TypeScript declarations.
 *
 * Usage: bun run build.ts
 */
import { $ } from "bun";

const ENTRY_POINTS = ["./src/index.ts", "./src/cli/generate-types.ts"];
const OUT_DIR = "./dist";

// Clean
await $`rm -rf ${OUT_DIR}`;

// ESM build
await Bun.build({
  entrypoints: ENTRY_POINTS,
  outdir: OUT_DIR,
  format: "esm",
  target: "bun",
  splitting: true,
  sourcemap: "linked",
  // external: ["@nuvix/cache"],
  minify: true,
  naming: "[dir]/[name].js",
});

console.log("✅ ESM build complete");

// CJS build
await Bun.build({
  entrypoints: ENTRY_POINTS,
  outdir: OUT_DIR,
  format: "cjs",
  target: "bun",
  splitting: false,
  sourcemap: "linked",
  // external: ["@nuvix/cache"],
  minify: true,
  naming: "[dir]/[name].cjs",
});

console.log("✅ CJS build complete");

// Type declarations via tsc
await $`bunx tsc --emitDeclarationOnly --declaration --outDir ${OUT_DIR}`.quiet();

console.log("✅ Type declarations generated");
console.log("🎉 Build finished → " + OUT_DIR);
