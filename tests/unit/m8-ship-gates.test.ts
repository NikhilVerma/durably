import { describe, expect, it } from "bun:test";
import { readFile } from "node:fs/promises";
import { encode } from "gpt-tokenizer";
import ts from "typescript";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";

const readmePath = new URL("../../README.md", import.meta.url);
const decisionsPath = new URL("../../DECISIONS.md", import.meta.url);
const packagePath = new URL("../../package.json", import.meta.url);

const extractFence = (source: string, marker: string): string => {
  const markerAt = source.indexOf(marker);
  if (markerAt < 0) throw new Error(`README marker not found: ${marker}`);
  const fenceEnd = source.indexOf("````", markerAt);
  if (fenceEnd < 0) throw new Error(`README fence not closed after: ${marker}`);
  return source.slice(markerAt, fenceEnd);
};

describe("M8 README and agent-context release gates", () => {
  it("keeps the AGENTS CORE contract within the hard 2,000-token budget", async () => {
    const readme = await readFile(readmePath, "utf8");
    const core = extractFence(readme, "AGENTS.md — durably CORE");
    const tokens = encode(core).length;

    expect(tokens).toBeGreaterThan(0);
    expect(tokens).toBeLessThanOrEqual(2_000);
  });

  it("keeps AGENTS EXTENDED within its 1,500-token disclosure budget", async () => {
    const readme = await readFile(readmePath, "utf8");
    const extended = extractFence(readme, "AGENTS-EXTENDED");
    const tokens = encode(extended).length;

    expect(tokens).toBeGreaterThan(0);
    expect(tokens).toBeLessThanOrEqual(1_500);
  });

  it("keeps CORE self-contained and signposts EXTENDED instead of duplicating it", async () => {
    const readme = await readFile(readmePath, "utf8");
    const core = extractFence(readme, "AGENTS.md — durably CORE");
    const extended = extractFence(readme, "AGENTS-EXTENDED");

    for (const essential of ["workflow", "run", "step", "parallel", "retry", "testEngine"]) {
      expect(core).toContain(essential);
    }
    for (const progressive of ["loop", "stash", "spawn", "waitFor", "resource", "StorageAdapter"]) {
      expect(extended).toContain(progressive);
    }
    expect(core).toContain("advisories");
    expect(core.length + extended.length).toBeLessThan(readme.length);
  });

  it("documents every initial advisory code in the agent-facing contract", async () => {
    const readme = await readFile(readmePath, "utf8");
    const extended = extractFence(readme, "AGENTS-EXTENDED");
    const codes = [
      "LOOP_SUGGESTED",
      "FANOUT_CEILING",
      "RETRY_STORM",
      "STASH_SUGGESTED",
      "BUDGET_NEAR",
      "WAITFOR_NO_TIMEOUT",
      "SNAPSHOT_HEAVY",
      "SLOW_STEP_NO_TIMEOUT",
    ];

    for (const code of codes) expect(extended).toContain(code);
    expect(new Set(codes).size).toBe(codes.length);
  });

  it("contains no obsolete eslint-plugin-durably recommendation", async () => {
    const readme = await readFile(readmePath, "utf8");
    expect(readme).not.toContain("eslint-plugin-durably");
    expect(readme).toContain("shadow replay");
  });

  it("classifies every TypeScript block and semantically compiles every concrete block", async () => {
    const readme = await readFile(readmePath, "utf8");
    const all = [...readme.matchAll(/```ts\n([\s\S]*?)```/g)].map((match, index) => ({
      index,
      source: match[1]!,
    }));
    const classified = all.filter(({ source }) => /\{\s*\.\.\.\s*\}|pseudo-code|omitted/i.test(source))
      .map(({ index }) => ({ index, reason: "contains an explicit ellipsis or pseudocode marker" }));
    const concrete = all.filter(({ index }) => !classified.some((item) => item.index === index));
    expect(concrete.length + classified.length).toBe(all.length);
    expect(classified.every((item) => item.reason.length > 10)).toBe(true);
    expect(concrete.length).toBeGreaterThanOrEqual(10);

    const dir = await mkdtemp(join(process.cwd(), ".readme-ts-"));
    try {
      const prelude = join(dir, "prelude.d.ts");
      await writeFile(prelude, `
        type ExampleWorkflow = import("durably").Workflow<unknown, unknown>;
        type ExampleSchema = import("durably").StandardSchemaV1<unknown, unknown>;
        type ToolCall = { name: string; args: Record<string, unknown> };
        type Generated = { text: string; finishReason: string; toolCalls: ToolCall[]; msgs: unknown[] };
        declare const model: unknown, urls: string[], input: unknown, seed: unknown, tools: readonly unknown[];
        declare const llm: {
          extract(value: unknown): Promise<unknown>;
          classify(value: unknown): Promise<{ kind: "escalate" | "normal" }>;
        };
        declare const inventory: {
          hold(sku: string): Promise<{ holdId: string }>;
          release(holdId: string): Promise<void>;
        };
        declare const backfill: ExampleWorkflow, processOne: ExampleWorkflow, digest: ExampleWorkflow;
        declare const agentTask: ExampleWorkflow, streamTask: ExampleWorkflow;
        declare function streamText(value: unknown): AsyncIterable<string>;
        declare function flaky(value: { signal: AbortSignal }): Promise<string>;
        declare function fetchPage(value: string): Promise<string>;
        declare function scrape(value: string): Promise<string>;
        declare const url: string, items: unknown[], sku: string;
        declare const ApprovalSchema: ExampleSchema, Triage: ExampleSchema, Decision: ExampleSchema;
        declare const ticket: unknown, triage: { kind: "escalate" | "normal" };
        declare const staleRun: { runId: string }, goal: unknown, ResearchInput: ExampleSchema;
        declare const Extraction: ExampleSchema, doc: unknown;
        declare const openai: ReturnType<typeof import("durably").resource>;
        declare function it(name: string, fn: () => Promise<void> | void): void;
        declare function expect(value: unknown): {
          toBe(value: unknown): void; toBeGreaterThan(value: number): void;
        };
        declare module "ai" { export function generateText(value: unknown): Promise<Generated>; }
        declare function generateText(value: unknown): Promise<Generated>;
        declare function execTool(call: ToolCall): Promise<unknown>;
        declare function notify(error: unknown): void;
      `);
      const files = [prelude];
      for (const block of concrete) {
        const path = join(dir, `block-${block.index}.ts`);
        const body = /^\s*import\s/m.test(block.source)
          ? block.source
          : `import { workflow, durably } from "durably";\nconst readmeFragment = workflow(async (ctx) => {\n${block.source}\n});`;
        await writeFile(path, `/// <reference path="./prelude.d.ts" />\n${body}\nexport {};\n`);
        files.push(path);
      }
      const program = ts.createProgram(files, {
        strict: true,
        noEmit: true,
        target: ts.ScriptTarget.ES2022,
        module: ts.ModuleKind.ESNext,
        moduleResolution: ts.ModuleResolutionKind.Bundler,
        types: ["node"],
        skipLibCheck: true,
      });
      const diagnostics = ts.getPreEmitDiagnostics(program).map((diagnostic) => ({
        file: diagnostic.file?.fileName,
        line: diagnostic.file && diagnostic.start !== undefined
          ? diagnostic.file.getLineAndCharacterOfPosition(diagnostic.start).line + 1
          : undefined,
        message: ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n"),
      }));
      expect(diagnostics).toEqual([]);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it("maps every advisory code to a cliff, implemented move, and resolvable docs route", async () => {
    const [readme, decisions] = await Promise.all([
      readFile(readmePath, "utf8"),
      readFile(decisionsPath, "utf8"),
    ]);
    const expectedRoutes: Record<string, string> = {
      LOOP_SUGGESTED: "loop",
      FANOUT_CEILING: "fanout",
      RETRY_STORM: "resources",
      STASH_SUGGESTED: "stash",
      BUDGET_NEAR: "budgets",
      WAITFOR_NO_TIMEOUT: "wait-for",
      SNAPSHOT_HEAVY: "loop#snapshot-size",
      SLOW_STEP_NO_TIMEOUT: "timeouts",
    };
    const readmeProof: Record<string, RegExp> = {
      loop: /ctx\.loop/,
      fanout: /Fan-out \/ fan-in/i,
      resources: /Shared resources/i,
      stash: /Layer 2 — `stash`/,
      budgets: /^### Budgets$/m,
      "wait-for": /human-in-the-loop/i,
      timeouts: /Retries, backoff, timeouts/i,
      "snapshot-size": /id="snapshot-size"/,
    };

    for (const [code, route] of Object.entries(expectedRoutes)) {
      const row = decisions.split("\n").find((line) => line.includes(`\`${code}\``));
      expect(row).toBeDefined();
      expect(row).toContain(`durably.dev/${route}`);
      expect(row!.split("|").filter((cell) => cell.trim()).length).toBeGreaterThanOrEqual(4);
      for (const segment of route.split(/[#/]/)) {
        expect(readmeProof[segment]?.test(readme)).toBe(true);
      }
    }
  });

  it("keeps run(): T and advisory-channel governance explicit", async () => {
    const decisions = await readFile(decisionsPath, "utf8");
    expect(decisions).toContain("`run(): T` is inviolable");
    expect(decisions).toContain("`onAdvisory`");
    expect(decisions).toContain("`onStart");
    expect(decisions).toContain("deferred");
  });

  it("declares core, test, and isolated sqlite package export surfaces", async () => {
    const pkg = JSON.parse(await readFile(packagePath, "utf8")) as {
      exports?: Record<string, unknown>;
    };
    expect(pkg.exports).toEqual(expect.objectContaining({
      ".": expect.anything(),
      "./test": expect.anything(),
      "./sqlite": expect.anything(),
    }));
    expect(JSON.stringify(pkg.exports?.["."])).not.toMatch(/sqlite/i);
    expect(JSON.stringify(pkg.exports?.["./sqlite"])).toMatch(/sqlite/i);
  });
});
