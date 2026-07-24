import { describe, expect, it } from "bun:test";
import {
  FileStorage,
  MemoryStorage,
  StaleRunError,
  createEngine,
  run,
  workflow,
  type LoopCompletion,
  type LoopIterationContext,
  type WorkflowContext,
} from "../../index.ts";
import { withTempDir } from "../support/kit.ts";

const failedResult = (result: { readonly ok: boolean }): boolean => !result.ok;

describe("M5 policy-only workflow edits", () => {
  it("resumes a failed parallel run under a lower concurrency without re-executing completed steps", async () => {
    const engine = createEngine({ storage: new MemoryStorage() });
    const executions = Array.from({ length: 10 }, () => 0);
    let recovering = false;
    let active = 0;
    let peak = 0;

    const execute = async (index: number) => {
      active++;
      peak = Math.max(peak, active);
      await Promise.resolve();
      active--;
      executions[index]!++;
      if (!recovering && index >= 5) throw new Error(`failed-${index}`);
      return index;
    };
    const batch = async (ctx: WorkflowContext, concurrency: number) => {
      const results = await ctx.parallel(
        executions.map((_, index) => () =>
          ctx.step(() => execute(index), { name: `task-${index}` })
        ),
        { concurrency },
      );
      if (results.some(failedResult)) throw new Error("batch failed");
      return results;
    };
    const before = workflow({ name: "parallel-policy-edit" }, (ctx) =>
      batch(ctx, 10)
    );
    const after = workflow({ name: "parallel-policy-edit" }, (ctx) =>
      batch(ctx, 2)
    );

    await engine.start();
    try {
      const first = await engine.enqueue(before, undefined, { key: "job" });
      await engine.drain();
      expect((await engine.inspect(first.runId))?.status).toBe("failed");
      expect(executions).toEqual([1, 1, 1, 1, 1, 1, 1, 1, 1, 1]);

      recovering = true;
      active = 0;
      peak = 0;
      const resumed = await engine.enqueue(after, undefined, { key: "job" });
      await engine.drain();

      expect(resumed.runId).toBe(first.runId);
      expect((await engine.inspect(first.runId))?.status).toBe("completed");
      expect(executions).toEqual([1, 1, 1, 1, 1, 2, 2, 2, 2, 2]);
      expect(peak).toBe(2);
    } finally {
      await engine.stop();
    }
  });

  it("applies changed retry, backoff, timeout, and keyed-concurrency policies to the failed step", async () => {
    const engine = createEngine({ storage: new MemoryStorage() });
    let executions = 0;
    const effect = async () => {
      executions++;
      if (executions < 3) throw new Error("transient");
      return "recovered";
    };
    const before = workflow({ name: "step-policy-edit" }, (ctx) =>
      ctx.step(effect, {
        name: "remote-call",
        retry: { attempts: 1, backoff: "exponential", baseMs: 10 },
        timeoutMs: 10,
        concurrencyKey: "old-key",
        concurrencyLimit: 10,
      })
    );
    const after = workflow({ name: "step-policy-edit" }, (ctx) =>
      ctx.step(effect, {
        name: "remote-call",
        retry: { attempts: 3, backoff: "none", jitter: false },
        timeoutMs: 1_000,
        concurrencyKey: "new-key",
        concurrencyLimit: 1,
      })
    );

    await engine.start();
    try {
      const first = await engine.enqueue(before, undefined, { key: "job" });
      await engine.drain();
      expect((await engine.inspect(first.runId))?.status).toBe("failed");
      expect(executions).toBe(1);

      const resumed = await engine.enqueue(after, undefined, { key: "job" });
      await engine.drain();

      expect(resumed.runId).toBe(first.runId);
      expect((await engine.inspect(first.runId))?.result).toBe("recovered");
      expect(executions).toBe(3);
    } finally {
      await engine.stop();
    }
  });

  it("uses an increased timeout on the next attempt instead of discarding the run", async () => {
    const engine = createEngine({ storage: new MemoryStorage() });
    let executions = 0;
    const slowEffect = ({ signal }: { readonly signal: AbortSignal }) =>
      new Promise<string>((resolve, reject) => {
        executions++;
        const timer = setTimeout(() => resolve("finished"), 15);
        signal.addEventListener("abort", () => {
          clearTimeout(timer);
          reject(signal.reason);
        }, { once: true });
      });
    const before = workflow({ name: "timeout-policy-edit" }, (ctx) =>
      ctx.step(slowEffect, { name: "slow", timeoutMs: 1 })
    );
    const after = workflow({ name: "timeout-policy-edit" }, (ctx) =>
      ctx.step(slowEffect, { name: "slow", timeoutMs: 100 })
    );

    await engine.start();
    try {
      const first = await engine.enqueue(before, undefined, { key: "job" });
      await engine.drain();
      expect((await engine.inspect(first.runId))?.status).toBe("failed");

      const resumed = await engine.enqueue(after, undefined, { key: "job" });
      await engine.drain();

      expect(resumed.runId).toBe(first.runId);
      expect((await engine.inspect(first.runId))?.result).toBe("finished");
      expect(executions).toBe(2);
    } finally {
      await engine.stop();
    }
  });

  it("resumes loop snapshots after snapshot and iteration limits change", async () => {
    const engine = createEngine({ storage: new MemoryStorage() });
    const executions = [0, 0, 0];
    const reduce = async (
      state: number,
      { done, step }: LoopIterationContext,
    ) => {
      if (state >= 3) return done(state);
      await step(() => {
        executions[state]!++;
        return state;
      }, { name: `iteration-${state}` });
      return state + 1;
    };
    const before = workflow({ name: "loop-policy-edit" }, (ctx) =>
      ctx.loop<number, number | LoopCompletion<number>>(
        0,
        reduce,
        { snapshotEvery: 1, maxIterations: 2 },
      )
    );
    const after = workflow({ name: "loop-policy-edit" }, (ctx) =>
      ctx.loop<number, number | LoopCompletion<number>>(
        0,
        reduce,
        { snapshotEvery: 2, maxIterations: 5 },
      )
    );

    await engine.start();
    try {
      const first = await engine.enqueue(before, undefined, { key: "job" });
      await engine.drain();
      expect((await engine.inspect(first.runId))?.status).toBe("failed");

      const resumed = await engine.enqueue(after, undefined, { key: "job" });
      await engine.drain();

      expect(resumed.runId).toBe(first.runId);
      expect((await engine.inspect(first.runId))?.result).toBe(3);
      expect(executions).toEqual([1, 1, 1]);
    } finally {
      await engine.stop();
    }
  });

  it("updates a persisted wait timeout without changing the signal identity", async () => {
    const engine = createEngine({ storage: new MemoryStorage() });
    const before = workflow({ name: "wait-policy-edit" }, (ctx) =>
      ctx.waitFor("approval")
    );
    const after = workflow({ name: "wait-policy-edit" }, (ctx) =>
      ctx.waitFor("approval", { timeoutMs: 60_000 })
    );

    await engine.start();
    try {
      const first = await engine.enqueue(before, undefined, { key: "job" });
      await engine.drain();
      const beforeState = await engine.inspect(first.runId) as
        | ({ waitingFor?: { timeoutAt?: number } })
        | null;
      expect(beforeState?.waitingFor?.timeoutAt).toBeUndefined();

      const resumed = await engine.enqueue(after, undefined, { key: "job" });
      const afterState = await engine.inspect(first.runId) as
        | ({ waitingFor?: { timeoutAt?: number } })
        | null;

      expect(resumed.runId).toBe(first.runId);
      expect(afterState?.waitingFor?.timeoutAt).toBeNumber();
    } finally {
      await engine.stop();
    }
  });

  it("resumes through the public run() file fast path after an inline policy edit", async () => {
    await withTempDir(async (dir) => {
      let stableExecutions = 0;
      let flakyExecutions = 0;
      let recovering = false;
      const stable = async () => {
        stableExecutions++;
        return "stable";
      };
      const flaky = async () => {
        flakyExecutions++;
        if (!recovering) throw new Error("transient");
        return "recovered";
      };
      const before = workflow(async (ctx) => {
        await ctx.step(stable, { name: "stable" });
        return ctx.step(flaky, {
          name: "flaky",
          retry: { attempts: 1, backoff: "exponential", baseMs: 100 },
        });
      });
      const after = workflow(async (ctx) => {
        await ctx.step(stable, { name: "stable" });
        return ctx.step(flaky, {
          name: "flaky",
          retry: { attempts: 3, backoff: "none" },
        });
      });

      await expect(run(before, undefined, { dir })).rejects.toThrow("transient");
      recovering = true;

      expect(await run(after, undefined, { dir })).toBe("recovered");
      expect(stableExecutions).toBe(1);
      expect(flakyExecutions).toBe(2);
    });
  });

  it("finds the structurally matching anonymous run when another failed run has the same input", async () => {
    await withTempDir(async (dir) => {
      let alphaStableExecutions = 0;
      let alphaFailingExecutions = 0;
      let betaExecutions = 0;
      let alphaRecovering = false;
      const alphaBefore = workflow(async (ctx) => {
        await ctx.step(() => {
          alphaStableExecutions++;
          return "alpha-stable";
        }, { name: "alpha-stable" });
        return ctx.step(() => {
          alphaFailingExecutions++;
          throw new Error("alpha failed");
        }, {
          name: "alpha-failing",
          retry: { attempts: 1, backoff: "exponential", baseMs: 10 },
        });
      });
      const alphaAfter = workflow(async (ctx) => {
        await ctx.step(() => {
          alphaStableExecutions++;
          return "alpha-stable";
        }, { name: "alpha-stable" });
        return ctx.step(() => {
          alphaFailingExecutions++;
          if (!alphaRecovering) throw new Error("alpha failed");
          return "alpha-recovered";
        }, {
          name: "alpha-failing",
          retry: { attempts: 2, backoff: "none" },
        });
      });
      const beta = workflow(async (ctx) =>
        ctx.step(() => {
          betaExecutions++;
          throw new Error("beta failed");
        }, { name: "beta-failing" })
      );

      await expect(run(alphaBefore, undefined, { dir })).rejects.toThrow("alpha failed");
      await expect(run(beta, undefined, { dir })).rejects.toThrow("beta failed");
      alphaRecovering = true;

      expect(await run(alphaAfter, undefined, { dir })).toBe("alpha-recovered");
      expect(alphaStableExecutions).toBe(1);
      expect(alphaFailingExecutions).toBe(2);
      expect(betaExecutions).toBe(1);
    });
  });

  it("scopes the same explicit key across anonymous workflow bodies and resumes the compatible one", async () => {
    await withTempDir(async (dir) => {
      let alphaStableExecutions = 0;
      let alphaFailingExecutions = 0;
      let betaExecutions = 0;
      let alphaRecovering = false;
      const alphaBefore = workflow(async (ctx) => {
        await ctx.step(() => {
          alphaStableExecutions++;
          return "alpha-stable";
        }, { name: "alpha-stable" });
        return ctx.step(() => {
          alphaFailingExecutions++;
          throw new Error("alpha failed");
        }, {
          name: "alpha-failing",
          retry: { attempts: 1, backoff: "exponential", baseMs: 10 },
        });
      });
      const alphaAfter = workflow(async (ctx) => {
        await ctx.step(() => {
          alphaStableExecutions++;
          return "alpha-stable";
        }, { name: "alpha-stable" });
        return ctx.step(() => {
          alphaFailingExecutions++;
          if (!alphaRecovering) throw new Error("alpha failed");
          return "alpha-recovered";
        }, {
          name: "alpha-failing",
          retry: { attempts: 2, backoff: "none" },
        });
      });
      const beta = workflow(async (ctx) =>
        ctx.step(() => {
          betaExecutions++;
          throw new Error("beta failed");
        }, { name: "beta-failing" })
      );
      const options = { dir, key: "shared-key" };

      await expect(run(alphaBefore, undefined, options)).rejects.toThrow("alpha failed");
      await expect(run(beta, undefined, options)).rejects.toThrow("beta failed");
      alphaRecovering = true;

      expect(await run(alphaAfter, undefined, options)).toBe("alpha-recovered");
      expect(alphaStableExecutions).toBe(1);
      expect(alphaFailingExecutions).toBe(2);
      expect(betaExecutions).toBe(1);
    });
  });

  it("persists policy-compatible identity across a file-engine restart", async () => {
    await withTempDir(async (dir) => {
      let executions = 0;
      const effect = async () => {
        executions++;
        if (executions === 1) throw new Error("first attempt fails");
        return "ok";
      };
      const before = workflow({ name: "file-policy-edit" }, (ctx) =>
        ctx.step(effect, {
          name: "effect",
          retry: { attempts: 1, backoff: "exponential", baseMs: 100 },
        })
      );
      const after = workflow({ name: "file-policy-edit" }, (ctx) =>
        ctx.step(effect, {
          name: "effect",
          retry: { attempts: 2, backoff: "none" },
        })
      );

      const firstEngine = createEngine({ storage: new FileStorage(dir) });
      await firstEngine.start();
      const first = await firstEngine.enqueue(before, undefined, { key: "job" });
      await firstEngine.drain();
      await firstEngine.stop();

      const secondEngine = createEngine({ storage: new FileStorage(dir) });
      await secondEngine.start();
      try {
        const resumed = await secondEngine.enqueue(after, undefined, { key: "job" });
        await secondEngine.drain();

        expect(resumed.runId).toBe(first.runId);
        expect((await secondEngine.inspect(first.runId))?.result).toBe("ok");
        expect(executions).toBe(2);
      } finally {
        await secondEngine.stop();
      }
    });
  });

  it("allows a failed step implementation to change while preserving completed siblings", async () => {
    const engine = createEngine({ storage: new MemoryStorage() });
    let completedExecutions = 0;
    let failedExecutions = 0;
    const before = workflow({ name: "effect-edit" }, async (ctx) => {
      await ctx.step(() => {
        completedExecutions++;
        return "kept";
      }, { name: "completed" });
      return ctx.step(() => {
        failedExecutions++;
        throw new Error("old implementation");
      }, { name: "editable" });
    });
    const after = workflow({ name: "effect-edit" }, async (ctx) => {
      await ctx.step(() => {
        completedExecutions++;
        return "kept";
      }, { name: "completed" });
      return ctx.step(() => {
        failedExecutions++;
        return "new implementation";
      }, { name: "editable" });
    });

    await engine.start();
    try {
      const first = await engine.enqueue(before, undefined, { key: "job" });
      await engine.drain();
      const resumed = await engine.enqueue(after, undefined, { key: "job" });
      await engine.drain();

      expect(resumed.runId).toBe(first.runId);
      expect((await engine.inspect(first.runId))?.result).toBe("new implementation");
      expect(completedExecutions).toBe(1);
      expect(failedExecutions).toBe(2);
    } finally {
      await engine.stop();
    }
  });
});

describe("M5 structurally incompatible workflow edits", () => {
  it("rejects a new branch inserted into an already-recorded parallel operation", async () => {
    const engine = createEngine({ storage: new MemoryStorage() });
    let insertedExecutions = 0;
    const before = workflow({ name: "parallel-topology-edit" }, async (ctx) => {
      await ctx.parallel([
        () => ctx.step(() => Promise.resolve("a"), { name: "a" }),
        () => ctx.step(() => Promise.resolve("b"), { name: "b" }),
      ]);
      return ctx.waitFor("continue");
    });
    const after = workflow({ name: "parallel-topology-edit" }, async (ctx) => {
      await ctx.parallel([
        () => ctx.step(() => Promise.resolve("a"), { name: "a" }),
        () => ctx.step(() => Promise.resolve("b"), { name: "b" }),
        () => ctx.step(() => {
          insertedExecutions++;
          return "inserted";
        }, { name: "inserted" }),
      ]);
      return ctx.waitFor("continue");
    });

    await engine.start();
    try {
      const first = await engine.enqueue(before, undefined, { key: "job" });
      await engine.drain();

      await expect(engine.enqueue(after, undefined, { key: "job" })).rejects.toBeInstanceOf(StaleRunError);
      expect((await engine.inspect(first.runId))?.status).toBe("stale");
      expect(insertedExecutions).toBe(0);
    } finally {
      await engine.stop();
    }
  });

  it("parks a run as stale when a durable step is inserted before recorded history", async () => {
    const engine = createEngine({ storage: new MemoryStorage() });
    const before = workflow({ name: "topology-edit" }, async (ctx) => {
      await ctx.step(() => Promise.resolve("recorded"), { name: "recorded" });
      return ctx.waitFor("continue");
    });
    const after = workflow({ name: "topology-edit" }, async (ctx) => {
      await ctx.step(() => Promise.resolve("inserted"), { name: "inserted" });
      await ctx.step(() => Promise.resolve("recorded"), { name: "recorded" });
      return ctx.waitFor("continue");
    });

    await engine.start();
    try {
      const first = await engine.enqueue(before, undefined, { key: "job" });
      await engine.drain();

      await expect(engine.enqueue(after, undefined, { key: "job" })).rejects.toBeInstanceOf(StaleRunError);
      expect((await engine.inspect(first.runId))?.status).toBe("stale");
    } finally {
      await engine.stop();
    }
  });

  it("rejects a schema edit that no longer accepts a recorded result", async () => {
    const engine = createEngine({ storage: new MemoryStorage() });
    const rejectingSchema = {
      "~standard": {
        version: 1 as const,
        vendor: "durably-tests",
        validate: () => ({ issues: [{ message: "recorded value rejected" }] }),
      },
    };
    const before = workflow({ name: "schema-edit" }, async (ctx) => {
      await ctx.step(() => Promise.resolve("recorded"), { name: "value" });
      return ctx.waitFor("continue");
    });
    const after = workflow({ name: "schema-edit" }, async (ctx) => {
      await ctx.step(() => Promise.resolve("recorded"), {
        name: "value",
        schema: rejectingSchema,
      });
      return ctx.waitFor("continue");
    });

    await engine.start();
    try {
      const first = await engine.enqueue(before, undefined, { key: "job" });
      await engine.drain();

      await expect(engine.enqueue(after, undefined, { key: "job" })).rejects.toBeInstanceOf(StaleRunError);
      expect((await engine.inspect(first.runId))?.status).toBe("stale");
    } finally {
      await engine.stop();
    }
  });

  it("rejects compensation changes for an effect that has already completed", async () => {
    const engine = createEngine({ storage: new MemoryStorage() });
    let compensatedBy = "";
    const before = workflow({ name: "compensation-edit" }, async (ctx) => {
      await ctx.step(() => Promise.resolve("recorded"), {
        name: "effect",
        compensate: async () => {
          compensatedBy = "old compensation";
        },
      });
      return ctx.waitFor("continue");
    });
    const after = workflow({ name: "compensation-edit" }, async (ctx) => {
      await ctx.step(() => Promise.resolve("recorded"), {
        name: "effect",
        compensate: async () => {
          compensatedBy = "new compensation";
        },
      });
      return ctx.waitFor("continue");
    });

    await engine.start();
    try {
      const first = await engine.enqueue(before, undefined, { key: "job" });
      await engine.drain();

      await expect(engine.enqueue(after, undefined, { key: "job" })).rejects.toBeInstanceOf(StaleRunError);
      expect((await engine.inspect(first.runId))?.status).toBe("stale");
      expect(compensatedBy).toBe("");
    } finally {
      await engine.stop();
    }
  });

  it("validates recorded loop iterations before the latest snapshot", async () => {
    const engine = createEngine({ storage: new MemoryStorage() });
    const before = workflow({ name: "snapshotted-loop-topology" }, async (ctx) => {
      await ctx.loop<number, number | LoopCompletion<number>>(
        0,
        async (state, { done, step }) => {
          if (state >= 2) return done(state);
          await step(() => Promise.resolve(state), { name: `iteration-${state}` });
          return state + 1;
        },
        { snapshotEvery: 1 },
      );
      return ctx.waitFor("continue");
    });
    const after = workflow({ name: "snapshotted-loop-topology" }, async (ctx) => {
      await ctx.loop<number, number | LoopCompletion<number>>(
        0,
        async (state, { done, step }) => {
          if (state >= 2) return done(state);
          await step(() => Promise.resolve(state), { name: `renamed-${state}` });
          return state + 1;
        },
        { snapshotEvery: 2 },
      );
      return ctx.waitFor("continue");
    });

    await engine.start();
    try {
      const first = await engine.enqueue(before, undefined, { key: "job" });
      await engine.drain();

      await expect(engine.enqueue(after, undefined, { key: "job" })).rejects.toBeInstanceOf(StaleRunError);
      expect((await engine.inspect(first.runId))?.status).toBe("stale");
    } finally {
      await engine.stop();
    }
  });
});
