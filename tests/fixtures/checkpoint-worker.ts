import { appendFileSync } from "node:fs";
import { run, workflow } from "../../index.ts";

// argv: <dir> <ledger> <killAfterStep> <checkpointEvery>
const [dir, ledger, killAfter, checkpointEvery] = process.argv.slice(2);
const killAfterStep = Number(killAfter);

const harvest = workflow<{ steps: number }>()(async (ctx, { steps }) => {
  let total = 0;
  for (let index = 0; index < steps; index++) {
    total += await ctx.step(() => {
      // The ledger records real executions, so a resume that re-runs recorded
      // work is visible as a duplicate line rather than inferred.
      appendFileSync(ledger!, `${index}\n`);
      if (index === killAfterStep) process.kill(process.pid, "SIGKILL");
      return Promise.resolve(index);
    }, { name: `batch-${index}` });
  }
  return total;
});

const result = await run(harvest, { steps: 300 }, {
  dir: dir!,
  checkpointEvery: Number(checkpointEvery),
});
process.stdout.write(`completed ${result}\n`);
