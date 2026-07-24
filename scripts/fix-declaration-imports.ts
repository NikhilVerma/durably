import { readdir, readFile, unlink, writeFile } from "node:fs/promises";
import { join } from "node:path";

const root = join(process.cwd(), "dist", "types");

const visit = async (directory: string): Promise<void> => {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      await visit(path);
      continue;
    }
    if (!entry.name.endsWith(".d.ts")) continue;
    const source = await readFile(path, "utf8");
    await writeFile(
      path,
      source.replace(/(from\s+["'][^"']+)\.ts(["'])/g, "$1.js$2"),
    );
  }
};

await visit(root);

for (const [relative, target] of [
  ["test.js", "./index.js"],
  ["test.cjs", "./index.cjs"],
  ["sqlite.js", "./index.js"],
  ["sqlite.cjs", "./index.cjs"],
] as const) {
  const path = join(process.cwd(), "dist", relative);
  const source = await readFile(path, "utf8");
  await writeFile(path, source.replaceAll("./index.ts", target));
}

for (const relative of [
  "index.d.ts",
  "test.d.ts",
  "sqlite.d.ts",
  "src/runtime.d.ts",
]) {
  const path = join(root, relative);
  const source = await readFile(path, "utf8");
  await writeFile(
    path.replace(/\.d\.ts$/, ".d.cts"),
    source.replace(/(from\s+["'][^"']+)\.js(["'])/g, "$1.cjs$2"),
  );
}

for (const relative of [
  "src/advisories.d.ts",
  "src/advisories/registry.d.ts",
  "src/docs/routes.d.ts",
  "src/errors/registry.d.ts",
  "src/policies/backoff.d.ts",
]) {
  await unlink(join(root, relative)).catch((error: NodeJS.ErrnoException) => {
    if (error.code !== "ENOENT") throw error;
  });
}
