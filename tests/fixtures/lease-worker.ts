import { appendFile, access } from "node:fs/promises";
import { constants } from "node:fs";
import { FileStorage, createEngine, workflow } from "../../index.ts";

const [mode, dir] = process.argv.slice(2);
if ((mode !== "owner" && mode !== "subscriber") || !dir) {
  throw new Error("usage: lease-worker.ts <owner|subscriber> <dir>");
}

const exists = async (path: string): Promise<boolean> => {
  try {
    await access(path, constants.F_OK);
    return true;
  } catch {
    return false;
  }
};

const wf = workflow<{ job: string }>()(async (ctx, input) => {
  await ctx.step(async () => {
    await appendFile(`${dir}/charges.txt`, `${input.job}\n`);
    return "charged";
  }, { name: "charge" });
  return ctx.step(async ({ signal }) => {
    await appendFile(`${dir}/second-attempts.txt`, `${process.pid}\n`);
    while (!(await exists(`${dir}/release`))) {
      if (signal.aborted) throw signal.reason;
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    return "done";
  }, { name: "finish" });
});

const engine = createEngine({
  storage: new FileStorage(dir),
  leaseMs: 40,
  heartbeatMs: 10,
  concurrency: 1,
});
await engine.start();
const handle = await engine.enqueue(wf, { job: "job-1" }, { key: "shared-job" });
process.stdout.write(`${mode.toUpperCase()} ${JSON.stringify({ runId: handle.runId, pid: process.pid })}\n`);
if (mode === "subscriber") {
  setTimeout(() => process.stdout.write(`HELD ${JSON.stringify({ pid: process.pid })}\n`), 25);
}
await engine.drain();
process.stdout.write(`DONE ${JSON.stringify(await engine.inspect(handle.runId))}\n`);
await engine.stop();
