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
