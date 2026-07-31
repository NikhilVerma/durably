import { describe, expect, it } from "bun:test";
import { MemoryStorage, createEngine, run, workflow } from "../../index.ts";
import { withTempDir } from "../support/kit.ts";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const peakTracker = () => {
  let live = 0;
  let peak = 0;
  return {
    async hold<T>(fn: () => Promise<T>): Promise<T> {
      live++;
      peak = Math.max(peak, live);
      try {
        return await fn();
      } finally {
        live--;
      }
    },
    get peak() {
      return peak;
    },
  };
};

describe("M9 a run parked on its children does not hold an execution slot", () => {
  it("does not deadlock when every slot is held by a parent waiting on a child", async () => {
    const child = workflow<{ id: string }>()(async (ctx, { id }) =>
      ctx.step(async () => {
        await sleep(5);
        return id;
      }, { name: "child-work" })
    );
    const parent = workflow<{ id: string }>()(async (ctx, { id }) => {
      const handle = await ctx.spawn(child, { id: `${id}-kid` });
      const [outcome] = await ctx.joinAll([handle]);
      return outcome?.ok === true ? outcome.value : "failed";
    });
    const engine = createEngine({ storage: new MemoryStorage(), concurrency: 2 });
    await engine.start();

    const handles = await Promise.all(
      ["a", "b", "c"].map((id) => engine.enqueue(parent, { id })),
    );
    const finished = await Promise.race([
      engine.drain().then(() => "drained" as const),
      sleep(5_000).then(() => "deadlocked" as const),
    ]);

    expect(finished).toBe("drained");
    for (const handle of handles) {
      const state = await engine.inspect(handle.runId);
      expect(state?.status).toBe("completed");
    }
    await engine.stop();
  }, 15_000);

  it("lets children use the whole cap instead of the cap minus its parent", async () => {
    const probe = peakTracker();
    const child = workflow<{ index: number }>()(async (ctx, { index }) =>
      ctx.step(() => probe.hold(async () => {
        await sleep(40);
        return index;
      }), { name: "child-work" })
    );
    const parent = workflow<{ lanes: number }>()(async (ctx, { lanes }) => {
      const kids = await ctx.spawnAll(
        Array.from({ length: lanes }, (_item, index) => [child, { index }] as const),
      );
      return (await ctx.joinAll(kids)).length;
    });
    const engine = createEngine({ storage: new MemoryStorage(), concurrency: 4 });
    await engine.start();

    const handle = await engine.enqueue(parent, { lanes: 8 });
    await engine.drain();

    expect((await engine.inspect(handle.runId))?.status).toBe("completed");
    expect(probe.peak).toBe(4);
    await engine.stop();
  }, 15_000);

  it("advises when a fan-out is wider than the engine will run at once", async () => {
    const child = workflow<{ index: number }>()(async (ctx, { index }) =>
      ctx.step(() => Promise.resolve(index), { name: "child-work" })
    );
    const parent = workflow<{ lanes: number }>()(async (ctx, { lanes }) => {
      const kids = await ctx.spawnAll(
        Array.from({ length: lanes }, (_item, index) => [child, { index }] as const),
      );
      return (await ctx.joinAll(kids)).length;
    });
    const engine = createEngine({ storage: new MemoryStorage(), concurrency: 2 });
    await engine.start();

    const handle = await engine.enqueue(parent, { lanes: 6 });
    await engine.drain();
    const state = await engine.inspect(handle.runId);

    const advisory = state?.advisories.find((item) => item.code === "CONCURRENCY_CAPPED");
    expect(advisory).toBeDefined();
    expect(advisory?.msg).toMatch(/6/);
    expect(advisory?.msg).toMatch(/2/);
    expect(advisory?.hint).toMatch(/createEngine/);
    await engine.stop();
  }, 15_000);

  it("stays silent when the engine can run the whole fan-out at once", async () => {
    const child = workflow<{ index: number }>()(async (ctx, { index }) =>
      ctx.step(() => Promise.resolve(index), { name: "child-work" })
    );
    const parent = workflow(async (ctx) => {
      const kids = await ctx.spawnAll([[child, { index: 0 }], [child, { index: 1 }]]);
      return (await ctx.joinAll(kids)).length;
    });
    const engine = createEngine({ storage: new MemoryStorage(), concurrency: 8 });
    await engine.start();

    const handle = await engine.enqueue(parent, undefined);
    await engine.drain();

    const state = await engine.inspect(handle.runId);
    expect(state?.advisories.some((item) => item.code === "CONCURRENCY_CAPPED")).toBe(false);
    await engine.stop();
  }, 15_000);
});

describe("M9 checkpoint frequency is a stated tradeoff, not a silent one", () => {
  class CountingStorage extends MemoryStorage {
    checkpoints = 0;
    override async append(runId: string, event: unknown): Promise<void> {
      if ((event as { state?: unknown }).state) this.checkpoints++;
      return super.append(runId, event);
    }
  }

  const stepped = workflow<{ steps: number }>()(async (ctx, { steps }) => {
    let total = 0;
    for (let index = 0; index < steps; index++) {
      total += await ctx.step(() => Promise.resolve(index), { name: `batch-${index}` });
    }
    return total;
  });

  const checkpointsFor = async (checkpointEvery: number): Promise<number> => {
    const storage = new CountingStorage();
    const engine = createEngine({ storage, concurrency: 1, checkpointEvery });
    await engine.start();
    const handle = await engine.enqueue(stepped, { steps: 300 });
    await engine.drain();
    expect((await engine.inspect(handle.runId))?.status).toBe("completed");
    await engine.stop();
    return storage.checkpoints;
  };

  it("records every completed step when checkpointEvery is 1", async () => {
    expect(await checkpointsFor(1)).toBeGreaterThanOrEqual(300);
  }, 30_000);

  it("trades checkpoints away only in proportion to the interval asked for", async () => {
    const every50 = await checkpointsFor(50);
    expect(every50).toBeLessThan(await checkpointsFor(1) / 5);
    expect(every50).toBeGreaterThan(0);
  }, 30_000);

  it("defaults a long run to a gap that scales with the run, not a fixed 100", async () => {
    const storage = new CountingStorage();
    const engine = createEngine({ storage, concurrency: 1 });
    await engine.start();
    const handle = await engine.enqueue(stepped, { steps: 300 });
    await engine.drain();
    await engine.stop();

    expect((await engine.inspect(handle.runId))?.status).toBe("completed");
    // The default interval is one percent of the run — three steps here — so a
    // 300-step run checkpoints throughout instead of every hundredth step.
    expect(storage.checkpoints).toBeGreaterThan(150);
    expect(await checkpointsFor(100)).toBeLessThan(storage.checkpoints / 3);
  }, 30_000);
});

describe("M9 the event log stops growing with the square of the step count", () => {
  it("keeps a long run's log proportional to its state, not to steps squared", async () => {
    await withTempDir(async (dir) => {
      const wf = workflow<{ steps: number }>()(async (ctx, { steps }) => {
        let total = 0;
        for (let index = 0; index < steps; index++) {
          total += await ctx.step(() => Promise.resolve(index), { name: `batch-${index}` });
        }
        return total;
      });

      await run(wf, { steps: 200 }, { dir, checkpointEvery: 1 });

      const { readdir, stat } = await import("node:fs/promises");
      const { join } = await import("node:path");
      const runsRoot = join(dir, "runs");
      const shard = (await readdir(runsRoot))[0]!;
      const runId = (await readdir(join(runsRoot, shard)))[0]!;
      const events = await stat(join(runsRoot, shard, runId, "events.log"));
      const state = await stat(join(runsRoot, shard, runId, "state.json"));

      // Before compaction a 200-step run wrote ~9 MB of repeated full states.
      // The log may hold a snapshot plus recent frames; it must not hold 200 of them.
      expect(events.size).toBeLessThan(state.size * 12);
      expect(events.size).toBeLessThan(2_000_000);
    });
  }, 30_000);
});
