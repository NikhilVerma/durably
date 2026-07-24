const { mkdtemp, rm } = require("node:fs/promises");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { run, workflow } = require("durably");

(async () => {
  const dir = await mkdtemp(join(tmpdir(), "durably-node-cjs-"));
  try {
    const wf = workflow(async (ctx, input) => {
      const value = await ctx.step(() => Promise.resolve(input + 1), { name: "increment" });
      return { runtime: "cjs", value };
    });
    const result = await run(wf, 41, { dir });
    if (JSON.stringify(result) !== JSON.stringify({ runtime: "cjs", value: 42 })) {
      throw new Error(`unexpected CJS result: ${JSON.stringify(result)}`);
    }
    process.stdout.write("cjs-ok\n");
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
