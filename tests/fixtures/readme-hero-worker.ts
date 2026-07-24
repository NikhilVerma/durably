import { access, appendFile } from "node:fs/promises";
import { constants } from "node:fs";
import { readFile } from "node:fs/promises";
import { run, workflow } from "../../index.ts";

const dir = process.argv[2];
if (!dir) throw new Error("usage: readme-hero-worker.ts <dir>");

const exists = async (path: string) => {
  try {
    await access(path, constants.F_OK);
    return true;
  } catch {
    return false;
  }
};
const waitFor = async (path: string, signal: AbortSignal) => {
  while (!(await exists(path))) {
    if (signal.aborted) throw signal.reason;
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
};

const summarize = workflow<{ index: number }>()(async (ctx, { index }) => {
  const page = await ctx.step(async ({ signal }) => {
    await appendFile(`${dir}/entered-fetch.log`, `${index}\n`);
    await waitFor(`${dir}/release-fetch-${index}`, signal);
    await appendFile(`${dir}/fetch.log`, `${index}\n`);
    return `page-${index}`;
  }, { name: "fetch" });
  return ctx.step(async ({ signal }) => {
    await appendFile(`${dir}/entered-generate.log`, `${index}\n`);
    await waitFor(`${dir}/release-generate-${index}`, signal);
    await appendFile(`${dir}/generate.log`, `${index}\n`);
    const attempts = (await readFile(`${dir}/generate.log`, "utf8")).split("\n")
      .filter((line) => line === String(index)).length;
    if ((index === 1 || index === 3) && attempts === 1) throw new Error("flaky generate");
    return { index, summary: `summary:${page}` };
  }, { name: "generateText", retry: { attempts: 3, backoff: "none" } });
});

const digest = workflow<{ count: number }>()(async (ctx, { count }) => {
  const children = await ctx.spawnAll(
    Array.from({ length: count }, (_, index) => [summarize, { index }] as const),
  );
  const outcomes = await ctx.joinAll(children);
  const items = outcomes.filter((outcome) => outcome.ok).map((outcome) => outcome.value);
  const intro = await ctx.step(async () => {
    await appendFile(`${dir}/intro.log`, "intro\n");
    return `intro:${items.length}`;
  }, { name: "intro generateText" });
  return { intro, items, failed: outcomes.length - items.length };
});

const result = await run(digest, { count: 4 }, { dir });
process.stdout.write(`DONE ${JSON.stringify(result)}\n`);
