import { describe, expect, it } from "bun:test";
import { access, readFile, readdir } from "node:fs/promises";
import { readFileSync } from "node:fs";
import { constants } from "node:fs";
import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const packagePath = resolve(root, "package.json");

const targetOf = (entry: unknown, condition: "import" | "require"): string | undefined => {
  if (typeof entry === "string") return entry;
  if (!entry || typeof entry !== "object") return undefined;
  const record = entry as Record<string, unknown>;
  const selected = record[condition] ?? record.default;
  return typeof selected === "string" ? selected : undefined;
};

const exists = async (path: string): Promise<boolean> => {
  try {
    await access(path, constants.F_OK);
    return true;
  } catch {
    return false;
  }
};

describe("M8 modern Node portability", () => {
  it("keeps production modules free of Bun runtime imports and globals", async () => {
    const productionFiles = ["index.ts", "sqlite.ts"];
    for (const relative of productionFiles) {
      const path = resolve(root, relative);
      if (!(await exists(path))) continue;
      const source = await readFile(path, "utf8");
      expect(source).not.toMatch(/from\s+["']bun(?::|["'])/);
      expect(source).not.toMatch(/\bBun\./);
    }
  });

  it("points package conditions at existing ESM and CJS build artifacts", async () => {
    const pkg = JSON.parse(await readFile(packagePath, "utf8")) as {
      exports?: Record<string, unknown>;
    };
    const core = pkg.exports?.["."];
    const esm = targetOf(core, "import");
    const cjs = targetOf(core, "require");
    expect(esm).toBeDefined();
    expect(cjs).toBeDefined();
    expect(await exists(resolve(root, esm!))).toBe(true);
    expect(await exists(resolve(root, cjs!))).toBe(true);
  });

  it("requires Node 20+ unconditionally in package metadata", async () => {
    const pkg = JSON.parse(await readFile(packagePath, "utf8")) as { engines?: { node?: string } };
    expect(pkg.engines?.node).toBeDefined();
    expect(pkg.engines!.node).toMatch(/(?:>=|\^)\s*20/);
  });

  it("loads the built core from both Node ESM and CJS consumers when Node is available", () => {
    const node = process.env.NODE_BINARY ?? "node";
    const version = spawnSync(node, ["--version"], { encoding: "utf8" });
    if (version.error && (version.error as NodeJS.ErrnoException).code === "ENOENT") {
      const pkg = JSON.parse(readFileSync(packagePath, "utf8")) as { engines?: { node?: string } };
      expect(pkg.engines?.node).toBeDefined();
      return;
    }
    expect(version.status).toBe(0);

    const esm = spawnSync(node, [resolve(root, "tests/fixtures/node-esm-smoke.mjs")], {
      cwd: root,
      encoding: "utf8",
    });
    const cjs = spawnSync(node, [resolve(root, "tests/fixtures/node-cjs-smoke.cjs")], {
      cwd: root,
      encoding: "utf8",
    });
    expect({ status: esm.status, stderr: esm.stderr }).toEqual({ status: 0, stderr: "" });
    expect(esm.stdout).toBe("esm-ok\n");
    expect({ status: cjs.status, stderr: cjs.stderr }).toEqual({ status: 0, stderr: "" });
    expect(cjs.stdout).toBe("cjs-ok\n");
  });

  it("isolates node:sqlite to the sqlite subpath build", async () => {
    const pkg = JSON.parse(await readFile(packagePath, "utf8")) as {
      exports?: Record<string, unknown>;
    };
    const coreEsm = targetOf(pkg.exports?.["."], "import");
    const sqliteEsm = targetOf(pkg.exports?.["./sqlite"], "import");
    expect(coreEsm).toBeDefined();
    expect(sqliteEsm).toBeDefined();

    const coreSource = await readFile(resolve(root, coreEsm!), "utf8");
    const sqliteSource = await readFile(resolve(root, sqliteEsm!), "utf8");
    expect(coreSource).not.toContain("node:sqlite");
    expect(sqliteSource).toContain("node:sqlite");
  });

  it("scans the full emitted production artifact graph for Bun coupling", async () => {
    const pkg = JSON.parse(await readFile(packagePath, "utf8")) as { exports?: Record<string, unknown> };
    const roots = [
      targetOf(pkg.exports?.["."], "import"),
      targetOf(pkg.exports?.["."], "require"),
      targetOf(pkg.exports?.["./test"], "import"),
      targetOf(pkg.exports?.["./sqlite"], "import"),
    ].filter((value): value is string => Boolean(value));
    expect(roots.length).toBeGreaterThanOrEqual(4);
    const buildDir = resolve(root, "dist");
    const visit = async (dir: string): Promise<string[]> => {
      const files: string[] = [];
      for (const entry of await readdir(dir, { withFileTypes: true })) {
        const path = resolve(dir, entry.name);
        if (entry.isDirectory()) files.push(...await visit(path));
        else if (/\.(?:m?js|cjs)$/.test(entry.name)) files.push(path);
      }
      return files;
    };
    const artifacts = await visit(buildDir);
    expect(artifacts.length).toBeGreaterThanOrEqual(roots.length);
    for (const artifact of artifacts) {
      const source = await readFile(artifact, "utf8");
      expect(source).not.toMatch(/(?:from|require\()\s*["']bun(?::|["'])/);
      expect(source).not.toMatch(/\bBun\./);
    }
  });

  it("declares CI coverage for Node 20 and a current Node line", async () => {
    const workflow = await readFile(resolve(root, ".github/workflows/ci.yml"), "utf8");
    expect(workflow).toMatch(/node-version:[\s\S]*20/);
    expect(workflow).toMatch(/node-version:[\s\S]*(?:22|24|current)/);
    expect(workflow).toContain("bun run test:types");
  });
});
