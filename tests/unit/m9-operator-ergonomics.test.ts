import { describe, expect, it } from "bun:test";
import {
  KeyConflictError,
  MemoryStorage,
  createEngine,
  run,
  testEngine,
  workflow,
} from "../../index.ts";
import type { StepEvent } from "../../index.ts";
import { deferred, tick, withTempDir } from "../support/kit.ts";

describe("M9 undefined follows JSON semantics across the durable boundary", () => {
  it("accepts an absent optional input field written as undefined", async () => {
    const wf = workflow<{ urls: string[]; maxImages?: number }>()(async (ctx, input) =>
      ctx.step(() => Promise.resolve(input.maxImages ?? input.urls.length), { name: "count" })
    );

    const result = await testEngine().run(wf, { urls: ["a", "b"], maxImages: undefined });

    expect(result.status).toBe("completed");
    expect(result.result).toBe(2);
  });

  it("gives an explicitly-undefined field the same identity as an absent one", async () => {
    await withTempDir(async (dir) => {
      const gate = deferred<void>();
      let executions = 0;
      const wf = workflow<{ id: string; limit?: number }>()(async (ctx, input) =>
        ctx.step(async () => {
          executions++;
          await gate.promise;
          return input.id;
        }, { name: "effect" })
      );

      const absent = run(wf, { id: "x" }, { dir });
      await tick();
      const explicit = run(wf, { id: "x", limit: undefined }, { dir });
      await tick();
      gate.resolve();

      expect(await absent).toBe("x");
      expect(await explicit).toBe("x");
      expect(executions).toBe(1);
    });
  });

  it("drops undefined-valued properties from step output as JSON.stringify would", async () => {
    const wf = workflow(async (ctx) =>
      ctx.step(() => Promise.resolve({ id: "x", nickname: undefined }), { name: "optional-field" })
    );
    const te = testEngine();

    const crashed = await te.run(wf, undefined, { crashAfter: "optional-field" });
    const done = await te.resume(crashed.runId);

    expect(done.status).toBe("completed");
    expect(done.result).toEqual({ id: "x" });
    expect(Object.keys(done.result as object)).toEqual(["id"]);
  });

  it("normalizes an explicitly undefined array element to null like a hole", async () => {
    const wf = workflow(async (ctx) =>
      ctx.step(() => Promise.resolve([1, undefined, 3]), { name: "sparse-explicit" })
    );

    const result = await testEngine().run(wf, undefined);

    expect(result.status).toBe("completed");
    expect(result.result).toEqual([1, null, 3]);
  });

  it("still rejects values JSON cannot express at all", async () => {
    const wf = workflow(async (ctx) =>
      ctx.step(() => Promise.resolve({ cursor: 1n }), { name: "bigint" })
    );

    const result = await testEngine().run(wf, undefined);

    expect(result.status).toBe("failed");
    expect(result.steps[0]?.error.path).toMatch(/cursor/);
  });
});

describe("M9 key conflicts name the bound run and both inputs", () => {
  it("reports the stored input, the offered input, and the run holding the key", async () => {
    const engine = createEngine({ storage: new MemoryStorage(), concurrency: 1 });
    const wf = workflow<{ maxImages: number }>()(async (ctx, input) =>
      ctx.step(() => Promise.resolve(input.maxImages), { name: "harvest" })
    );
    await engine.start();
    const first = await engine.enqueue(wf, { maxImages: 1000 }, { key: "steam-harvest-v1" });

    const conflict = await engine
      .enqueue(wf, { maxImages: 0 }, { key: "steam-harvest-v1" })
      .then(() => undefined, (error: unknown) => error as KeyConflictError);

    expect(conflict).toBeInstanceOf(KeyConflictError);
    expect(conflict!.message).toContain("steam-harvest-v1");
    expect(conflict!.message).toContain(`{"maxImages":1000}`);
    expect(conflict!.message).toContain(`{"maxImages":0}`);
    expect(conflict!.message).toContain(first.runId);
    expect(conflict!.key).toBe("steam-harvest-v1");
    expect(conflict!.runId).toBe(first.runId);
    expect(conflict!.boundInput).toEqual({ maxImages: 1000 });
    expect(conflict!.input).toEqual({ maxImages: 0 });
    expect(conflict!.hint).toMatch(/inspect|fresh/i);
  });

  it("carries the same detail through the direct run() path", async () => {
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

      const conflict = await run(wf, { customer: "b" }, { key: "stable-key", dir })
        .then(() => undefined, (error: unknown) => error as KeyConflictError);

      expect(conflict).toBeInstanceOf(KeyConflictError);
      expect(conflict!.boundInput).toEqual({ customer: "a" });
      expect(conflict!.input).toEqual({ customer: "b" });
      expect(conflict!.runId).toMatch(/.+/);
      gate.resolve();
      expect(await first).toBe("a");
    });
  });
});

describe("M9 runs are findable by the key their operator chose", () => {
  it("filters list() by key and reports the key on run state", async () => {
    const engine = createEngine({ storage: new MemoryStorage(), concurrency: 1 });
    const wf = workflow<{ id: number }>()(async (ctx, input) =>
      ctx.step(() => Promise.resolve(input.id), { name: "work" })
    );
    await engine.start();
    const wanted = await engine.enqueue(wf, { id: 1 }, { key: "harvest-v1" });
    await engine.enqueue(wf, { id: 2 }, { key: "harvest-v2" });
    await engine.drain();

    const found = await engine.list({ key: "harvest-v1" });

    expect(found.map((state) => state.runId)).toEqual([wanted.runId]);
    expect(found[0]?.key).toBe("harvest-v1");
    expect((await engine.list()).length).toBe(2);
    expect(await engine.list({ key: "absent" })).toEqual([]);
  });
});

describe("M9 step progress is observable without parsing the event log", () => {
  it("reports each attempt, the retry between attempts, and the outcome", async () => {
    const events: StepEvent[] = [];
    let attempts = 0;
    const wf = workflow(async (ctx) => {
      await ctx.step(() => {
        attempts++;
        if (attempts === 1) throw new Error("transient");
        return Promise.resolve("ok");
      }, { name: "flaky", retry: { attempts: 2, backoff: "none" } });
      return ctx.step(() => Promise.resolve("done"), { name: "final" });
    });

    const result = await testEngine().run(wf, undefined, { onStep: (event) => events.push(event) });

    expect(result.status).toBe("completed");
    expect(events.map((event) => `${event.label}:${event.status}`)).toEqual([
      "flaky:running",
      "flaky:retrying",
      "flaky:running",
      "flaky:ok",
      "final:running",
      "final:ok",
    ]);
    expect(events.every((event) => event.runId === result.runId)).toBe(true);
    expect(events[1]?.attempt).toBe(1);
    expect(events[1]?.error).toBeInstanceOf(Error);
    expect(events[2]?.attempt).toBe(2);
    expect(events.find((event) => event.status === "ok")?.ms).toBeGreaterThanOrEqual(0);
    expect(events[0]?.path).toEqual([0]);
    expect(events.at(-1)?.path).toEqual([1]);
  });

  it("distinguishes a step replayed from the log from one that executed", async () => {
    const te = testEngine();
    const wf = workflow(async (ctx) => {
      const first = await ctx.step(() => Promise.resolve(1), { name: "first" });
      const second = await ctx.step(() => Promise.resolve(2), { name: "second" });
      return first + second;
    });

    const crashed = await te.run(wf, undefined, { crashAfter: "first" });
    const events: StepEvent[] = [];
    const done = await te.resume(crashed.runId, { onStep: (event) => events.push(event) });

    expect(done.status).toBe("completed");
    expect(events.map((event) => `${event.label}:${event.status}`)).toEqual([
      "first:replayed",
      "second:running",
      "second:ok",
    ]);
    expect(done.steps.every((step) => step.executions === 1)).toBe(true);
  });

  it("reports a terminal step failure and survives a throwing observer", async () => {
    const events: StepEvent[] = [];
    const wf = workflow(async (ctx) =>
      ctx.step(() => Promise.reject(new Error("permanent")), { name: "doomed" })
    );

    const result = await testEngine().run(wf, undefined, {
      onStep: (event) => {
        events.push(event);
        throw new Error("observer exploded");
      },
    });

    expect(result.status).toBe("failed");
    expect(events.map((event) => event.status)).toEqual(["running", "failed"]);
    expect(events[1]?.error).toBeInstanceOf(Error);
  });

  it("streams progress from the public run() entry point", async () => {
    await withTempDir(async (dir) => {
      const events: StepEvent[] = [];
      const wf = workflow(async (ctx) => ctx.step(() => Promise.resolve("value"), { name: "only" }));

      const result = await run(wf, undefined, { dir, onStep: (event) => events.push(event) });

      expect(result).toBe("value");
      expect(events.map((event) => event.status)).toEqual(["running", "ok"]);
      expect(events[0]?.label).toBe("only");
    });
  });
});
