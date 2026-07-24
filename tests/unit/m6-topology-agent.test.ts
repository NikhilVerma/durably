import { describe, expect, it } from "bun:test";
import {
  MemoryStorage,
  RunCancelledError,
  createEngine,
  testEngine,
  workflow,
} from "../../index.ts";
import { objectWithStringIdSchema, seededDelays } from "../support/kit.ts";

describe("M6 topology: parallel paths and result values", () => {
  it("returns failures as Result values without rejecting sibling branches", async () => {
    const wf = workflow(async (ctx) =>
      ctx.parallel([
        () => ctx.step(() => Promise.resolve("a"), { name: "a" }),
        () => ctx.step(() => Promise.reject(new Error("broken")), { name: "broken" }),
        () => ctx.step(() => Promise.resolve("c"), { name: "c" }),
      ])
    );

    const result = await testEngine().run(wf, undefined);
    expect(result.status).toBe("completed");
    expect(result.result).toEqual([
      { ok: true, value: "a" },
      { ok: false, error: expect.objectContaining({ message: "broken" }) },
      { ok: true, value: "c" },
    ]);
  });

  it("preserves input ordering when branches finish out of order", async () => {
    const delays = [30, 5, 20, 0, 10];
    const wf = workflow(async (ctx) =>
      ctx.parallel(delays.map((delay, index) => async () => {
        await new Promise((resolve) => setTimeout(resolve, delay));
        return ctx.step(() => Promise.resolve(index), { name: `branch-${index}` });
      }))
    );

    const result = await testEngine().run(wf, undefined);
    expect(result.result).toEqual(delays.map((_, value) => ({ ok: true, value })));
  });

  it("limits started branches with the parallel concurrency option", async () => {
    let active = 0;
    let peak = 0;
    const wf = workflow(async (ctx) =>
      ctx.parallel(Array.from({ length: 12 }, (_, index) => async () => {
        active++;
        peak = Math.max(peak, active);
        await Promise.resolve();
        active--;
        return ctx.step(() => Promise.resolve(index), { name: `branch-${index}` });
      }), { concurrency: 3 })
    );

    const result = await testEngine().run(wf, undefined);
    expect(result.status).toBe("completed");
    expect(peak).toBe(3);
  });

  it("cancellation prevents queued parallel thunks from starting", async () => {
    const gate = new Promise<void>(() => undefined);
    const started: number[] = [];
    const engine = createEngine({ storage: new MemoryStorage() });
    const wf = workflow(async (ctx) =>
      ctx.parallel(Array.from({ length: 10 }, (_, index) => async () => {
        started.push(index);
        if (index === 0) await gate;
        return ctx.step(() => Promise.resolve(index), { name: `branch-${index}` });
      }), { concurrency: 1 })
    );
    await engine.start();
    try {
      const handle = await engine.enqueue(wf, undefined);
      await Promise.resolve();
      await engine.cancel(handle.runId);
      expect(started).toEqual([0]);
      expect((await engine.inspect(handle.runId))?.status).toBe("cancelled");
    } finally {
      await engine.stop();
    }
  });

  it("assigns stable hierarchical paths to branches with unequal step counts", async () => {
    const wf = workflow(async (ctx) =>
      ctx.parallel([
        () => ctx.step(() => Promise.resolve("a0"), { name: "a0" }),
        async () => {
          await ctx.step(() => Promise.resolve("b0"), { name: "b0" });
          await ctx.step(() => Promise.resolve("b1"), { name: "b1" });
          return ctx.step(() => Promise.resolve("b2"), { name: "b2" });
        },
        async () => {
          await ctx.step(() => Promise.resolve("c0"), { name: "c0" });
          return ctx.step(() => Promise.resolve("c1"), { name: "c1" });
        },
      ])
    );

    const result = await testEngine().run(wf, undefined);
    expect(result.steps.map(({ label, path }) => ({ label, path }))).toEqual([
      { label: "a0", path: [0, 0, 0] },
      { label: "b0", path: [0, 1, 0] },
      { label: "b1", path: [0, 1, 1] },
      { label: "b2", path: [0, 1, 2] },
      { label: "c0", path: [0, 2, 0] },
      { label: "c1", path: [0, 2, 1] },
    ]);
  });

  it("keeps hierarchical paths stable across randomized completion order and replay", async () => {
    const delays = seededDelays(16);
    const wf = workflow(async (ctx) =>
      ctx.parallel(delays.map((delay, branch) => async () => {
        await new Promise((resolve) => setTimeout(resolve, delay));
        const values: number[] = [];
        for (let step = 0; step <= branch % 4; step++) {
          values.push(await ctx.step(() => Promise.resolve(branch * 10 + step), {
            name: `b${branch}s${step}`,
          }));
        }
        return values;
      }), { concurrency: 5 })
    );
    const te = testEngine();

    const first = await te.run(wf, undefined, { crashAfter: [0, 7, 1] });
    const done = await te.resume(first.runId);

    expect(done.status).toBe("completed");
    expect(done.steps.every((step) => step.executions === 1)).toBe(true);
    for (const step of done.steps) {
      expect(step.path[0]).toBe(0);
      expect(step.path).toHaveLength(3);
    }
  });

  it("nests branch-local path frames recursively", async () => {
    const wf = workflow(async (ctx) =>
      ctx.parallel([
        () => ctx.parallel([
          () => ctx.step(() => Promise.resolve("nested-0"), { name: "nested-0" }),
          () => ctx.step(() => Promise.resolve("nested-1"), { name: "nested-1" }),
        ]),
      ])
    );

    const result = await testEngine().run(wf, undefined);
    expect(result.steps.map((step) => step.path)).toEqual([
      [0, 0, 0, 0, 0],
      [0, 0, 0, 1, 0],
    ]);
  });
});

describe("M6 topology: child workflows", () => {
  const child = workflow<{ value: number }>(async (ctx, input) =>
    ctx.step(() => Promise.resolve(input.value * 2), { name: "child-effect" })
  );

  it("spawns a child as its own durable run and joins its output", async () => {
    const parent = workflow(async (ctx) => {
      const handle = await ctx.spawn(child, { value: 3 });
      const [result] = await ctx.joinAll([handle]);
      return { handle, result };
    });

    const result = await testEngine().run(parent, undefined);
    expect(result.result.result).toEqual({ ok: true, value: 6 });
    expect(result.result.handle.runId).not.toBe(result.runId);
  });

  it("spawnAll and joinAll retain one result per child in input order", async () => {
    const parent = workflow(async (ctx) => {
      const kids = await ctx.spawnAll([
        [child, { value: 3 }],
        [child, { value: 1 }],
        [child, { value: 2 }],
      ]);
      return ctx.joinAll(kids);
    });

    expect((await testEngine().run(parent, undefined)).result).toEqual([
      { ok: true, value: 6 },
      { ok: true, value: 2 },
      { ok: true, value: 4 },
    ]);
  });

  it("joinAll returns child failures as values", async () => {
    const failing = workflow(async (ctx) =>
      ctx.step(() => Promise.reject(new Error("child failed")), { name: "child-failure" })
    );
    const parent = workflow(async (ctx) => {
      const kids = await ctx.spawnAll([
        [child, { value: 2 }],
        [failing, undefined],
      ]);
      return ctx.joinAll(kids);
    });

    const result = await testEngine().run(parent, undefined);
    expect(result.status).toBe("completed");
    expect(result.result).toEqual([
      { ok: true, value: 4 },
      { ok: false, error: expect.objectContaining({ message: "child failed" }) },
    ]);
  });

  it("replay does not create a duplicate child run", async () => {
    const parent = workflow(async (ctx) => {
      const handle = await ctx.spawn(child, { value: 5 });
      await ctx.step(() => Promise.resolve(null), { name: "parent-checkpoint" });
      const [result] = await ctx.joinAll([handle]);
      return result;
    });
    const te = testEngine();

    const crashed = await te.run(parent, undefined, { crashAfter: "parent-checkpoint" });
    const childIdsBefore = crashed.children.map((handle) => handle.runId);
    const done = await te.resume(crashed.runId);

    expect(done.children.map((handle) => handle.runId)).toEqual(childIdsBefore);
    expect(new Set(done.children.map((handle) => handle.runId)).size).toBe(1);
  });

  it("derives stable child ids from parent run, spawn path, and child index", async () => {
    const parent = workflow(async (ctx) => {
      const handles = await ctx.spawnAll([
        [child, { value: 1 }],
        [child, { value: 2 }],
      ] as const);
      return handles.map((handle) => handle.runId);
    });
    const te = testEngine();
    const done = await te.run(parent, undefined);
    expect(done.result).toEqual([
      `${done.runId}:0:0`,
      `${done.runId}:0:1`,
    ]);
  });

  it("cancelling a parent cascades to non-detached children", async () => {
    const waitingChild = workflow(async (ctx) => ctx.waitFor("release"));
    const parent = workflow(async (ctx) => {
      await ctx.spawn(waitingChild, undefined);
      return ctx.waitFor("parent-release");
    });
    const engine = createEngine({ storage: new MemoryStorage() });
    await engine.start();
    const waiting = await engine.enqueue(parent, undefined);
    await engine.drain();
    await engine.cancel(waiting.runId);
    const state = await engine.inspect(waiting.runId);

    expect(state?.status).toBe("cancelled");
    const childState = await engine.inspect(state!.children[0]!);
    expect(childState?.status).toBe("cancelled");
    expect(state?.error).toBeInstanceOf(RunCancelledError);
    await engine.stop();
  });

  it("detached children survive parent cancellation", async () => {
    const waitingChild = workflow(async (ctx) => ctx.waitFor("release"));
    const parent = workflow(async (ctx) => {
      await ctx.spawn(waitingChild, undefined, { detached: true });
      return ctx.waitFor("parent-release");
    });
    const engine = createEngine({ storage: new MemoryStorage() });
    await engine.start();
    const waiting = await engine.enqueue(parent, undefined);
    await engine.drain();
    await engine.cancel(waiting.runId);
    const state = await engine.inspect(waiting.runId);

    expect(state?.status).toBe("cancelled");
    const childState = await engine.inspect(state!.children[0]!);
    expect(childState?.status).toBe("waiting");
    await engine.stop();
  });
});

describe("M6 agent primitives: loop, wait, sleep, stash", () => {
  it("loop returns the value passed to done and stops iterating", async () => {
    let iterations = 0;
    const wf = workflow(async (ctx) =>
      ctx.loop({ count: 0 }, async (state, { done }) => {
        iterations++;
        if (state.count === 4) return done({ answer: state.count });
        return { count: state.count + 1 };
      })
    );

    const result = await testEngine().run(wf, undefined);
    expect(result.result).toEqual({ answer: 4 });
    expect(iterations).toBe(5);
  });

  it("enforces maxIterations", async () => {
    const wf = workflow(async (ctx) =>
      ctx.loop({ count: 0 }, async (state) => ({ count: state.count + 1 }), {
        maxIterations: 3,
      })
    );

    const result = await testEngine().run(wf, undefined);
    expect(result.status).toBe("failed");
    expect(result.error).toEqual(expect.objectContaining({
      message: expect.stringMatching(/maxIterations|iteration/i),
    }));
  });

  it("resumes from the latest snapshot instead of replaying old reducers", async () => {
    const reducerCalls = new Map<number, number>();
    const wf = workflow(async (ctx) =>
      ctx.loop({ turn: 0 }, async (state, { step, done }) => {
        reducerCalls.set(state.turn, (reducerCalls.get(state.turn) ?? 0) + 1);
        const response = await step(() => Promise.resolve(state.turn + 1), {
          name: "generateText",
        });
        if (response === 50) return done(response);
        return { turn: response };
      }, { snapshotEvery: 10, maxIterations: 100 })
    );
    const te = testEngine();

    const crashed = await te.run(wf, undefined, {
      crashAfter: { label: "generateText", nth: 47 },
    });
    const done = await te.resume(crashed.runId);

    expect(done.result).toBe(50);
    for (let turn = 0; turn < 40; turn++) {
      expect(reducerCalls.get(turn)).toBe(1);
    }
  });

  it("validates a waitFor signal payload before resuming", async () => {
    const wf = workflow(async (ctx) =>
      ctx.waitFor("approval", objectWithStringIdSchema)
    );
    const te = testEngine();
    const waiting = await te.run(wf, undefined);

    await expect(te.signal(waiting.runId, "approval", { id: 42 })).rejects.toThrow(/string id|valid/i);
    expect((await te.resume(waiting.runId)).status).toBe("waiting");

    await te.signal(waiting.runId, "approval", { id: "operator-1" });
    expect((await te.resume(waiting.runId)).result).toEqual({ id: "operator-1" });
  });

  it("fails a waitFor after its durable timeout", async () => {
    const te = testEngine();
    const wf = workflow(async (ctx) =>
      ctx.waitFor("approval", undefined, { timeoutMs: 5_000 })
    );
    const waiting = await te.run(wf, undefined);

    te.clock.advance(4_999);
    expect((await te.resume(waiting.runId)).status).toBe("waiting");
    te.clock.advance(1);
    const timedOut = await te.resume(waiting.runId);

    expect(timedOut.status).toBe("failed");
    expect(timedOut.error).toEqual(expect.objectContaining({ timeoutMs: 5_000 }));
  });

  it("sleepUntil uses an absolute durable wake time", async () => {
    const te = testEngine();
    const wakeAt = new Date(te.clock.now() + 10_000);
    const wf = workflow(async (ctx) => {
      await ctx.sleepUntil(wakeAt);
      return "awake";
    });
    const sleeping = await te.run(wf, undefined);

    te.clock.advance(10_000);
    expect((await te.resume(sleeping.runId)).result).toBe("awake");
  });

  it("hands the latest stash to a retried interrupted step and clears it on completion", async () => {
    const seen: unknown[] = [];
    const wf = workflow(async (ctx) =>
      ctx.step(async ({ stash, stashed }) => {
        seen.push(stashed);
        if (stashed === undefined) {
          await stash({ text: "partial", offset: 7 });
        }
        return { resumedFrom: stashed ?? null };
      }, { name: "streamText" })
    );
    const te = testEngine();

    const crashed = await te.run(wf, undefined, { crashInStep: "streamText" });
    const done = await te.resume(crashed.runId);

    expect(seen).toEqual([undefined, { text: "partial", offset: 7 }]);
    expect(done.result).toEqual({ resumedFrom: { text: "partial", offset: 7 } });
    expect(done.steps[0]?.stashed).toBeUndefined();
  });

  it("annotation updates are shallow-merged into inspectable state", async () => {
    const wf = workflow(async (ctx) => {
      ctx.annotate({ phase: "reviewing", reviewed: 1 });
      ctx.annotate({ reviewed: 2, total: 10 });
      return ctx.waitFor("continue");
    });
    const engine = createEngine({ storage: new MemoryStorage() });
    await engine.start();
    const waiting = await engine.enqueue(wf, undefined);
    await engine.drain();
    const state = await engine.inspect(waiting.runId);

    expect(state?.annotations).toEqual({
      phase: "reviewing",
      reviewed: 2,
      total: 10,
    });
    await engine.stop();
  });
});
