import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { run, workflow } from "@nikhilverma/durably";

const dir = await mkdtemp(join(tmpdir(), "durably-node-esm-"));
try {
  const wf = workflow(async (ctx, input) => {
    const value = await ctx.step(() => Promise.resolve(input + 1), { name: "increment" });
    return { runtime: "esm", value };
  });
  const result = await run(wf, 41, { dir });
  if (JSON.stringify(result) !== JSON.stringify({ runtime: "esm", value: 42 })) {
    throw new Error(`unexpected ESM result: ${JSON.stringify(result)}`);
  }
  process.stdout.write("esm-ok\n");
} finally {
  await rm(dir, { recursive: true, force: true });
}
