import { execFileSync } from "node:child_process";
import {
  mkdtemp,
  readFile,
  readdir,
  rm,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";

const expectedFiles = [
  "LICENSE",
  "README.md",
  "dist/index.cjs",
  "dist/index.js",
  "dist/sqlite.cjs",
  "dist/sqlite.js",
  "dist/test.cjs",
  "dist/test.js",
  "dist/types/index.d.ts",
  "dist/types/index.d.cts",
  "dist/types/sqlite.d.ts",
  "dist/types/sqlite.d.cts",
  "dist/types/src/runtime.d.cts",
  "dist/types/src/runtime.d.ts",
  "dist/types/test.d.cts",
  "dist/types/test.d.ts",
  "package.json",
].sort();

const forbiddenContent = [
  { name: "workspace path", pattern: /\/Volumes\/repos\/keel|\/Users\/nikhilverma/ },
  { name: "private key", pattern: /-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/ },
  { name: "npm token", pattern: /\bnpm_[A-Za-z0-9]{30,}\b/ },
  { name: "GitHub token", pattern: /\bgh[opusr]_[A-Za-z0-9]{30,}\b/ },
  { name: "AWS access key", pattern: /\bAKIA[0-9A-Z]{16}\b/ },
];

const run = (
  command: string,
  args: string[],
  options: { cwd?: string; env?: NodeJS.ProcessEnv } = {},
): string =>
  execFileSync(command, args, {
    cwd: options.cwd,
    env: options.env ?? process.env,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });

const listFiles = async (root: string): Promise<string[]> => {
  const found: string[] = [];
  const visit = async (directory: string): Promise<void> => {
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) await visit(path);
      else found.push(relative(root, path));
    }
  };
  await visit(root);
  return found.sort();
};

const audit = async (): Promise<void> => {
  const directory = await mkdtemp(join(tmpdir(), "durably-package-audit-"));
  try {
    const packed = JSON.parse(run("npm", [
      "pack",
      "--json",
      "--ignore-scripts",
      "--pack-destination",
      directory,
      "--cache",
      join(directory, "npm-cache"),
    ])) as Array<{ filename: string }>;
    const filename = packed[0]?.filename;
    if (!filename) throw new Error("npm pack did not return a tarball filename");
    const tarball = join(directory, filename);
    const extracted = join(directory, "extracted");
    await import("node:fs/promises").then(({ mkdir }) => mkdir(extracted));
    run("tar", ["-xzf", tarball, "-C", extracted]);

    const packageRoot = join(extracted, "package");
    const files = await listFiles(packageRoot);
    if (JSON.stringify(files) !== JSON.stringify(expectedFiles)) {
      throw new Error(
        `package file allowlist mismatch\nexpected: ${expectedFiles.join("\n")}\nactual: ${files.join("\n")}`,
      );
    }

    for (const file of files) {
      const source = await readFile(join(packageRoot, file), "utf8");
      for (const forbidden of forbiddenContent) {
        if (forbidden.pattern.test(source)) {
          throw new Error(`${forbidden.name} leaked into package file ${file}`);
        }
      }
    }

    const consumer = join(directory, "consumer");
    run("npm", [
      "install",
      "--ignore-scripts",
      "--prefix",
      consumer,
      tarball,
      "--cache",
      join(directory, "npm-cache"),
    ]);

    const esm = `
      import { mkdtemp, rm } from "node:fs/promises";
      import { tmpdir } from "node:os";
      import { join } from "node:path";
      import { run, workflow } from "@nikhilverma/durably";
      import { testEngine } from "@nikhilverma/durably/test";
      import { SqliteStorage } from "@nikhilverma/durably/sqlite";
      const dir = await mkdtemp(join(tmpdir(), "durably-audit-esm-"));
      try {
        const wf = workflow(async (ctx, input) =>
          ctx.step(() => Promise.resolve(input + 1), { name: "increment" })
        );
        if (await run(wf, 41, { dir }) !== 42) throw new Error("ESM run failed");
        if (typeof testEngine !== "function") throw new Error("test subpath failed");
        if (typeof SqliteStorage !== "function") throw new Error("sqlite subpath failed");
      } finally {
        await rm(dir, { recursive: true, force: true });
      }
    `;
    run("node", ["--input-type=module", "--eval", esm], { cwd: consumer });

    const cjs = `
      const { mkdtemp, rm } = require("node:fs/promises");
      const { tmpdir } = require("node:os");
      const { join } = require("node:path");
      const { run, workflow } = require("@nikhilverma/durably");
      const { testEngine } = require("@nikhilverma/durably/test");
      const { SqliteStorage } = require("@nikhilverma/durably/sqlite");
      (async () => {
        const dir = await mkdtemp(join(tmpdir(), "durably-audit-cjs-"));
        try {
          const wf = workflow(async (ctx, input) =>
            ctx.step(() => Promise.resolve(input + 1), { name: "increment" })
          );
          if (await run(wf, 41, { dir }) !== 42) throw new Error("CJS run failed");
          if (typeof testEngine !== "function") throw new Error("test subpath failed");
          if (typeof SqliteStorage !== "function") throw new Error("sqlite subpath failed");
        } finally {
          await rm(dir, { recursive: true, force: true });
        }
      })().catch((error) => { console.error(error); process.exit(1); });
    `;
    run("node", ["--eval", cjs], { cwd: consumer });

    await writeFile(join(consumer, "consumer.ts"), `
      import { run, workflow, type Workflow } from "@nikhilverma/durably";
      import { testEngine } from "@nikhilverma/durably/test";
      import { SqliteStorage } from "@nikhilverma/durably/sqlite";
      const wf = workflow<number>()(async (ctx, input) =>
        ctx.step(() => Promise.resolve(input + 1))
      );
      const contract: Workflow<number, number> = wf;
      const result: Promise<number> = run(contract, 41);
      void result;
      void testEngine;
      void SqliteStorage;
    `);
    await writeFile(join(consumer, "tsconfig.json"), JSON.stringify({
      compilerOptions: {
        strict: true,
        noEmit: true,
        target: "ES2022",
        module: "NodeNext",
        moduleResolution: "NodeNext",
        lib: ["ES2022", "DOM"],
        skipLibCheck: false,
      },
      include: ["consumer.ts"],
    }, null, 2));
    run(join(process.cwd(), "node_modules", ".bin", "tsc"), ["-p", "tsconfig.json"], {
      cwd: consumer,
    });

    process.stdout.write(
      `package-audit-ok ${filename} ${files.length} allowlisted files; ESM, CJS, and TypeScript consumers passed\n`,
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
};

await audit();
