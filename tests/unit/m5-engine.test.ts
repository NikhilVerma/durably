import { describe, expect, it } from "bun:test";
import {
  KeyConflictError,
  MemoryStorage,
  StaleRunError,
  createEngine,
  run,
  workflow,
} from "../../index.ts";
import { deferred, tick, withTempDir } from "../support/kit.ts";

describe("M5 run(): join, start, and resume identity", () => {
  it("canonicalizes plain-object input keys before hashing identity", async () => {
    await withTempDir(async (dir) => {
      const gate = deferred<void>();
      let executions = 0;
      const wf = workflow<Record<string, number>>(async (ctx, input) =>
        ctx.step(async () => {
          executions++;
          await gate.promise;
          return Object.keys(input).sort();
        }, { name: "effect" })
      );

      const first = run(wf, { a: 1, b: 2 }, { dir });
      await tick();
      const joined = run(wf, { b: 2, a: 1 }, { dir });
      await tick();
      expect(executions).toBe(1);

      gate.resolve();
      expect(await first).toEqual(["a", "b"]);
      expect(await joined).toEqual(["a", "b"]);
      expect(executions).toBe(1);
    });
  });

  it("canonicalizes nested objects recursively while preserving array order", async () => {
    await withTempDir(async (dir) => {
      const gate = deferred<void>();
      let calls = 0;
      const wf = workflow<Record<string, unknown>>(async (ctx) =>
        ctx.step(async () => {
          calls++;
          await gate.promise;
          return "same";
        }, { name: "effect" })
      );
      const first = run(wf, { z: [{ b: 2, a: 1 }], a: { y: 2, x: 1 } }, { dir });
      await tick();
      const joined = run(wf, { a: { x: 1, y: 2 }, z: [{ a: 1, b: 2 }] }, { dir });
      await tick();
      expect(calls).toBe(1);
      gate.resolve();
      expect(await Promise.all([first, joined])).toEqual(["same", "same"]);
    });
  });

  it("scopes explicit keys per workflow rather than globally", async () => {
    await withTempDir(async (dir) => {
      const alpha = workflow({ name: "alpha" }, async () => "alpha");
      const beta = workflow({ name: "beta" }, async () => "beta");
      expect(await Promise.all([
        run(alpha, undefined, { dir, key: "same-key" }),
        run(beta, undefined, { dir, key: "same-key" }),
      ])).toEqual(["alpha", "beta"]);
    });
  });

  it("two concurrent callers subscribe to the same live lease", async () => {
    await withTempDir(async (dir) => {
      const gate = deferred<void>();
      let billedCalls = 0;
      const wf = workflow<{ prompt: string }>()(async (ctx) =>
        ctx.step(async () => {
          billedCalls++;
          await gate.promise;
          return "answer";
        }, { name: "generateText" })
      );

      const owner = run(wf, { prompt: "same" }, { dir });
      await tick();
      const subscriber = run(wf, { prompt: "same" }, { dir });
      await tick();

      expect(billedCalls).toBe(1);
      gate.resolve();
      expect(await Promise.all([owner, subscriber])).toEqual(["answer", "answer"]);
    });
  });

  it("a completed identity starts a fresh run instead of memoizing forever", async () => {
    await withTempDir(async (dir) => {
      let executions = 0;
      const wf = workflow(async (ctx) =>
        ctx.step(() => Promise.resolve(++executions), { name: "effect" })
      );

      expect(await run(wf, undefined, { dir })).toBe(1);
      expect(await run(wf, undefined, { dir })).toBe(2);
      expect(executions).toBe(2);
    });
  });

  it("a failed identity resumes at the failing step on the next invocation", async () => {
    await withTempDir(async (dir) => {
      const executions = { completed: 0, failed: 0 };
      let fixed = false;
      const wf = workflow(async (ctx) => {
        await ctx.step(() => Promise.resolve(++executions.completed), { name: "completed" });
        return ctx.step(() => {
          executions.failed++;
          if (!fixed) throw new Error("not fixed");
          return Promise.resolve("done");
        }, { name: "failed" });
      });

      await expect(run(wf, undefined, { dir })).rejects.toThrow("not fixed");
      fixed = true;
      expect(await run(wf, undefined, { dir })).toBe("done");
      expect(executions).toEqual({ completed: 1, failed: 2 });
    });
  });

  it("fresh:true starts over after failure including completed steps", async () => {
    await withTempDir(async (dir) => {
      const executions = { completed: 0, failed: 0 };
      let fixed = false;
      const wf = workflow(async (ctx) => {
        await ctx.step(() => Promise.resolve(++executions.completed), { name: "completed" });
        return ctx.step(() => {
          executions.failed++;
          if (!fixed) throw new Error("not fixed");
          return Promise.resolve("done");
        }, { name: "failed" });
      });

      await expect(run(wf, undefined, { dir })).rejects.toThrow("not fixed");
      fixed = true;
      expect(await run(wf, undefined, { dir, fresh: true })).toBe("done");
      expect(executions).toEqual({ completed: 2, failed: 2 });
    });
  });

  it("allows step callback implementation edits for future and retrying steps", async () => {
    await withTempDir(async (dir) => {
      let implementation = async (value: number) => {
        if (value === 2) throw new Error("old implementation");
        return `old:${value}`;
      };
      const executions = [0, 0];
      const wf = workflow(async (ctx) => {
        const first = await ctx.step(() => {
          executions[0]++;
          return implementation(1);
        }, { name: "first" });
        const second = await ctx.step(() => {
          executions[1]++;
          return implementation(2);
        }, { name: "second" });
        return [first, second];
      });
      await expect(run(wf, undefined, { dir })).rejects.toThrow("old implementation");
      implementation = async (value) => `new:${value}`;

      expect(await run(wf, undefined, { dir })).toEqual(["old:1", "new:2"]);
      expect(executions).toEqual([1, 2]);
    });
  });

  it("key replaces input identity but rejects conflicting input reuse", async () => {
    await withTempDir(async (dir) => {
      const gate = deferred<void>();
      const wf = workflow<{ customer: string }>()(async (ctx, input) =>
        ctx.step(async () => {
          await gate.promise;
          return input.customer;
        }, { name: "effect" })
      );
      const first = run(wf, { customer: "a" }, { key: "stable-key", dir });
      await tick();

      await expect(
        run(wf, { customer: "b" }, { key: "stable-key", dir }),
      ).rejects.toBeInstanceOf(KeyConflictError);
      gate.resolve();
      expect(await first).toBe("a");
    });
  });
});

describe("M5 engine lifecycle and operations", () => {
  it("enqueue returns a handle immediately and drain runs queued work", async () => {
    const engine = createEngine({ storage: new MemoryStorage(), concurrency: 1 });
    const wf = workflow<{ id: number }>()(async (ctx, input) =>
      ctx.step(() => Promise.resolve(input.id), { name: "work" })
    );
    await engine.start();

    const handle = await engine.enqueue(wf, { id: 7 });
    expect(handle).toEqual(expect.objectContaining({ runId: expect.any(String) }));
    await engine.drain();

    expect(await engine.inspect(handle.runId)).toEqual(expect.objectContaining({
      status: "completed",
      result: 7,
    }));
    await engine.stop();
  });

  it("claims higher priority first and FIFO within equal priority", async () => {
    const order: string[] = [];
    const engine = createEngine({ storage: new MemoryStorage(), concurrency: 1 });
    const wf = workflow<{ id: string }>()(async (ctx, input) =>
      ctx.step(() => {
        order.push(input.id);
        return Promise.resolve(input.id);
      }, { name: "work" })
    );

    await engine.enqueue(wf, { id: "low" }, { priority: -10 });
    await engine.enqueue(wf, { id: "normal-1" }, { priority: 0 });
    await engine.enqueue(wf, { id: "high" }, { priority: 10 });
    await engine.enqueue(wf, { id: "normal-2" }, { priority: 0 });
    await engine.start();
    await engine.drain();

    expect(order).toEqual(["high", "normal-1", "normal-2", "low"]);
    await engine.stop();
  });

  it("delayMs prevents claiming work before its due time", async () => {
    const engine = createEngine({ storage: new MemoryStorage() });
    let calls = 0;
    const wf = workflow(async (ctx) =>
      ctx.step(() => Promise.resolve(++calls), { name: "work" })
    );
    const handle = await engine.enqueue(wf, undefined, { delayMs: 1 });
    await engine.start();

    await engine.drain();
    expect(calls).toBe(0);
    expect((await engine.inspect(handle.runId))?.status).toBe("pending");
    await new Promise((resolve) => setTimeout(resolve, 2));
    await engine.drain();
    expect(calls).toBe(1);
    await engine.stop();
  });

  it("pause lands before the next step and resume continues", async () => {
    const gate = deferred<void>();
    const calls: string[] = [];
    const engine = createEngine({ storage: new MemoryStorage() });
    const wf = workflow(async (ctx) => {
      await ctx.step(async () => {
        calls.push("first");
        await gate.promise;
      }, { name: "first" });
      await ctx.step(() => {
        calls.push("second");
        return Promise.resolve();
      }, { name: "second" });
    });
    await engine.start();
    const handle = await engine.enqueue(wf, undefined);
    await tick();

    await engine.pause(handle.runId);
    gate.resolve();
    await engine.drain();
    expect(calls).toEqual(["first"]);
    expect((await engine.inspect(handle.runId))?.status).toBe("paused");

    await engine.resume(handle.runId);
    await engine.drain();
    expect(calls).toEqual(["first", "second"]);
    await engine.stop();
  });

  it("cancel aborts the active attempt signal and reaches cancelled", async () => {
    let signalWasAborted = false;
    const engine = createEngine({ storage: new MemoryStorage() });
    const wf = workflow(async (ctx) =>
      ctx.step(({ signal }) =>
        new Promise((_resolve, reject) => {
          signal.addEventListener("abort", () => {
            signalWasAborted = true;
            reject(signal.reason);
          }, { once: true });
        }), { name: "active" })
    );
    await engine.start();
    const handle = await engine.enqueue(wf, undefined);
    await tick();

    await engine.cancel(handle.runId);
    await engine.drain();

    expect(signalWasAborted).toBe(true);
    expect((await engine.inspect(handle.runId))?.status).toBe("cancelled");
    await engine.stop();
  });

  it("signal only releases a run waiting for that signal name", async () => {
    const engine = createEngine({ storage: new MemoryStorage() });
    const wf = workflow(async (ctx) => ctx.waitFor("approval"));
    await engine.start();
    const handle = await engine.enqueue(wf, undefined);
    await engine.drain();

    await engine.signal(handle.runId, "other", { ignored: true });
    await engine.drain();
    expect((await engine.inspect(handle.runId))?.status).toBe("waiting");

    await engine.signal(handle.runId, "approval", { by: "operator" });
    await engine.drain();
    expect(await engine.inspect(handle.runId)).toEqual(expect.objectContaining({
      status: "completed",
      result: { by: "operator" },
    }));
    await engine.stop();
  });

  it("inspect returns null for an unknown run", async () => {
    const engine = createEngine({ storage: new MemoryStorage() });
    await engine.start();
    expect(await engine.inspect("missing")).toBeNull();
    await engine.stop();
  });

  it("list filters by status, workflow, and limit", async () => {
    const engine = createEngine({ storage: new MemoryStorage() });
    const alpha = workflow({ name: "alpha" }, async () => "a");
    const beta = workflow({ name: "beta" }, async () => "b");
    await engine.enqueue(alpha, undefined);
    await engine.enqueue(alpha, undefined);
    await engine.enqueue(beta, undefined);

    const alphaPending = await engine.list({ status: "pending", workflow: "alpha", limit: 1 });
    expect(alphaPending).toHaveLength(1);
    expect(alphaPending[0]).toEqual(expect.objectContaining({
      status: "pending",
      workflow: "alpha",
    }));
  });

  it("retry fromStep invalidates the selected step and everything after it", async () => {
    const values = { one: 0, two: 0, three: 0 };
    let fail = true;
    const engine = createEngine({ storage: new MemoryStorage() });
    const wf = workflow(async (ctx) => {
      await ctx.step(() => Promise.resolve(++values.one), { name: "one" });
      await ctx.step(() => Promise.resolve(++values.two), { name: "two" });
      return ctx.step(() => {
        values.three++;
        if (fail) throw new Error("three failed");
        return Promise.resolve(values);
      }, { name: "three" });
    });
    await engine.start();
    const handle = await engine.enqueue(wf, undefined);
    await engine.drain();
    fail = false;

    await engine.retry(handle.runId, { fromStep: "two" });
    await engine.drain();

    expect(values).toEqual({ one: 1, two: 2, three: 2 });
    await engine.stop();
  });

  it("changed workflow bodies park unfinished runs as stale", async () => {
    const engine = createEngine({ storage: new MemoryStorage() });
    const before = workflow({ name: "versioned" }, async (ctx) => ctx.waitFor("continue"));
    await engine.start();
    const handle = await engine.enqueue(before, undefined, { key: "same" });
    await engine.drain();

    const after = workflow({ name: "versioned" }, async (ctx) => {
      await ctx.step(() => Promise.resolve("new"), { name: "new" });
      return ctx.waitFor("continue");
    });
    await expect(engine.enqueue(after, undefined, { key: "same" })).rejects.toBeInstanceOf(StaleRunError);
    expect((await engine.inspect(handle.runId))?.status).toBe("stale");
    await engine.stop();
  });

  it("adopt records the compatibility assertion and pins the new body", async () => {
    const engine = createEngine({ storage: new MemoryStorage() });
    const before = workflow({ name: "adoptable" }, async (ctx) => ctx.waitFor("continue"));
    await engine.start();
    const handle = await engine.enqueue(before, undefined, { key: "same" });
    await engine.drain();
    const after = workflow({ name: "adoptable" }, async (ctx) => {
      const marker = "new-body";
      return ctx.waitFor(`continue:${marker}`);
    });
    await engine.enqueue(after, undefined, { key: "same" }).catch(() => undefined);

    await engine.adopt(handle.runId);
    expect(await engine.inspect(handle.runId)).toEqual(expect.objectContaining({
      status: "waiting",
      adoption: expect.objectContaining({
        assertedCompatible: true,
        bodyHash: expect.any(String),
      }),
    }));
    await engine.stop();
  });

  it("restart creates a fresh run under changed code without reusing old steps", async () => {
    const engine = createEngine({ storage: new MemoryStorage() });
    const old = workflow({ name: "restartable" }, async (ctx) => ctx.waitFor("continue"));
    await engine.start();
    const oldHandle = await engine.enqueue(old, undefined, { key: "same" });
    await engine.drain();

    const changed = workflow({ name: "restartable" }, async (ctx) => {
      await ctx.step(() => Promise.resolve("changed"), { name: "changed" });
      return ctx.waitFor("continue");
    });
    await engine.enqueue(changed, undefined, { key: "same" }).catch(() => undefined);
    const newHandle = await engine.restart(oldHandle.runId);
    await engine.drain();

    expect(newHandle.runId).not.toBe(oldHandle.runId);
    expect((await engine.inspect(newHandle.runId))?.steps).toEqual([
      expect.objectContaining({ label: "changed", executions: 1 }),
    ]);
    await engine.stop();
  });
});
