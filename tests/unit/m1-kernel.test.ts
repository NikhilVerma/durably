import { describe, expect, it, spyOn } from "bun:test";
import {
  SerializationError,
  testEngine,
  workflow,
} from "../../index.ts";

describe("M1 kernel: steps and replay", () => {
  it("returns the workflow result and records the step output", async () => {
    const wf = workflow<{ value: number }>()(async (ctx, input) => {
      const doubled = await ctx.step(() => Promise.resolve(input.value * 2), { name: "double" });
      return doubled + 1;
    });

    const result = await testEngine().run(wf, { value: 4 });

    expect(result.status).toBe("completed");
    expect(result.result).toBe(9);
    expect(result.steps).toEqual([
      expect.objectContaining({
        path: [0],
        label: "double",
        output: 8,
        executions: 1,
        attempts: 1,
      }),
    ]);
  });

  it("assigns monotonically increasing top-level position paths", async () => {
    const wf = workflow(async (ctx) => {
      await ctx.step(() => Promise.resolve("a"), { name: "a" });
      await ctx.step(() => Promise.resolve("b"), { name: "b" });
      await ctx.step(() => Promise.resolve("c"), { name: "c" });
    });

    const result = await testEngine().run(wf, undefined);
    expect(result.steps.map((step) => step.path)).toEqual([[0], [1], [2]]);
  });

  it("does not execute completed effects again after a crash", async () => {
    const calls = [0, 0, 0];
    const wf = workflow(async (ctx) => {
      await ctx.step(() => Promise.resolve(++calls[0]!), { name: "first" });
      await ctx.step(() => Promise.resolve(++calls[1]!), { name: "second" });
      await ctx.step(() => Promise.resolve(++calls[2]!), { name: "third" });
      return calls.slice();
    });
    const te = testEngine();

    const crashed = await te.run(wf, undefined, { crashAfter: "second" });
    expect(crashed.status).toBe("crashed");
    const done = await te.resume(crashed.runId);

    expect(done.status).toBe("completed");
    expect(calls).toEqual([1, 1, 1]);
    expect(done.steps.every((step) => step.executions === 1)).toBe(true);
  });

  it("can crash and resume at every top-level boundary", async () => {
    for (const boundary of ["one", "two", "three"]) {
      const calls = new Map<string, number>();
      const wf = workflow(async (ctx) => {
        for (const name of ["one", "two", "three"]) {
          await ctx.step(() => {
            calls.set(name, (calls.get(name) ?? 0) + 1);
            return Promise.resolve(name);
          }, { name });
        }
        return "done";
      });
      const te = testEngine();

      const crashed = await te.run(wf, undefined, { crashAfter: boundary });
      const resumed = await te.resume(crashed.runId);

      expect(resumed.result).toBe("done");
      expect([...calls.values()]).toEqual([1, 1, 1]);
    }
  });

  it("records ctx.now and ctx.random so replay returns identical values", async () => {
    const wf = workflow(async (ctx) => ({
      before: ctx.now(),
      random: ctx.random(),
      after: await ctx.step(() => Promise.resolve(ctx.now()), { name: "checkpoint" }),
    }));
    const te = testEngine();

    const crashed = await te.run(wf, undefined, { crashAfter: "checkpoint" });
    te.clock.advance(50_000);
    const resumed = await te.resume(crashed.runId);

    expect(resumed.result.before).toBe(resumed.result.after);
    expect(resumed.result.random).toBeGreaterThanOrEqual(0);
    expect(resumed.result.random).toBeLessThan(1);
  });

  it("complete returns immediately without executing later code", async () => {
    let afterComplete = false;
    const wf = workflow(async (ctx) => {
      ctx.complete({ reason: "short-circuit" });
      afterComplete = true;
      return { reason: "wrong" };
    });

    const result = await testEngine().run(wf, undefined);
    expect(result.result).toEqual({ reason: "short-circuit" });
    expect(afterComplete).toBe(false);
  });

  it("records ctx.log once and keeps shadow replay silent", async () => {
    const output = spyOn(console, "log").mockImplementation(() => undefined);
    const wf = workflow(async (ctx) => {
      ctx.log("phase", 1);
      await ctx.step(() => Promise.resolve(null), { name: "checkpoint" });
      ctx.log("phase", 2);
    });
    const te = testEngine();

    await te.run(wf, undefined);

    expect(output.mock.calls).toEqual([["phase", 1], ["phase", 2]]);
    output.mockRestore();
  });

  it("auto-labels an unnamed step from its callback source", async () => {
    const wf = workflow(async (ctx) => {
      await ctx.step(() => Promise.resolve("recognizable-label-fragment"));
    });

    const result = await testEngine().run(wf, undefined);
    expect(result.steps[0]?.label).toContain("recognizable-label-fragment");
  });

  it("allows duplicate labels because paths remain canonical identity", async () => {
    const wf = workflow(async (ctx) => {
      await ctx.step(() => Promise.resolve(1), { name: "same" });
      await ctx.step(() => Promise.resolve(2), { name: "same" });
    });

    const result = await testEngine().run(wf, undefined);
    expect(result.steps.map(({ label, path }) => ({ label, path }))).toEqual([
      { label: "same", path: [0] },
      { label: "same", path: [1] },
    ]);
  });

  it("fails loudly when replay takes a different control-flow path", async () => {
    let executions = 0;
    const wf = workflow(async (ctx) => {
      executions++;
      await ctx.step(() => Promise.resolve("first"), { name: "first" });
      if (executions === 1) {
        await ctx.step(() => Promise.resolve("conditional"), { name: "conditional" });
      }
      await ctx.step(() => Promise.resolve("last"), { name: "last" });
    });
    const te = testEngine();
    await expect(te.run(wf, undefined)).rejects.toEqual(expect.objectContaining({
      path: expect.any(Array),
      expected: expect.anything(),
      actual: expect.anything(),
    }));
  });

  it.each([
    ["function", () => "effect"],
    ["symbol", Symbol("effect")],
    ["bigint", 1n],
    ["map", new Map([["x", 1]])],
  ])("rejects non-serializable %s step output at the producing step", async (_kind, value) => {
    const wf = workflow(async (ctx) =>
      ctx.step(() => Promise.resolve(value), { name: "invalid-output" })
    );

    const result = await testEngine().run(wf, undefined);

    expect(result.status).toBe("failed");
    expect(result.steps[0]?.error).toBeInstanceOf(SerializationError);
    expect(result.steps[0]?.error).toEqual(expect.objectContaining({ path: expect.anything() }));
  });

  it("round-trips nested JSON values and Date values across replay", async () => {
    const output = {
      nested: { list: [null, true, 3.5, "x"] },
      at: new Date("2026-07-24T10:12:03.000Z"),
    };
    const wf = workflow(async (ctx) =>
      ctx.step(() => Promise.resolve(output), { name: "serializable-output" })
    );
    const te = testEngine();

    const crashed = await te.run(wf, undefined, { crashAfter: "serializable-output" });
    const done = await te.resume(crashed.runId);

    expect(done.result).toEqual(output);
    expect(done.result?.at).toBeInstanceOf(Date);
  });

  it.each([NaN, Infinity, -Infinity])("rejects non-finite numeric output %p", async (value) => {
    const wf = workflow(async (ctx) =>
      ctx.step(() => Promise.resolve({ nested: { value } }), { name: "non-finite" })
    );
    const result = await testEngine().run(wf, undefined);
    expect(result.steps[0]?.error).toBeInstanceOf(SerializationError);
    expect(result.steps[0]?.error.path).toMatch(/nested.*value/);
  });

  it("rejects cyclic output with the precise nested path", async () => {
    const cyclic: { child?: unknown } = {};
    cyclic.child = cyclic;
    const wf = workflow(async (ctx) =>
      ctx.step(() => Promise.resolve({ payload: cyclic }), { name: "cycle" })
    );
    const result = await testEngine().run(wf, undefined);
    expect(result.steps[0]?.error).toBeInstanceOf(SerializationError);
    expect(result.steps[0]?.error.path).toMatch(/payload.*child/);
  });

  it("rejects class instances even when their enumerable properties are JSON-shaped", async () => {
    class Identifier {
      constructor(readonly value: string) {}
    }
    const wf = workflow(async (ctx) =>
      ctx.step(() => Promise.resolve({ id: new Identifier("x") }), { name: "class-instance" })
    );
    const result = await testEngine().run(wf, undefined);
    expect(result.steps[0]?.error).toBeInstanceOf(SerializationError);
    expect(result.steps[0]?.error.path).toMatch(/id/);
  });

  it("rejects non-serializable workflow input before executing any effects", async () => {
    let calls = 0;
    const wf = workflow<{ payload: unknown }>()(async (ctx) =>
      ctx.step(() => Promise.resolve(++calls), { name: "must-not-run" })
    );
    await expect(
      testEngine().run(wf, { payload: { callback: () => undefined } }),
    ).rejects.toEqual(expect.objectContaining({
      path: expect.stringMatching(/payload.*callback/),
    }));
    expect(calls).toBe(0);
  });

  it("rejects non-serializable annotations at the annotation boundary", async () => {
    const wf = workflow(async (ctx) => {
      ctx.annotate({ nested: { invalid: Symbol("x") } });
      return "unreachable";
    });
    const result = await testEngine().run(wf, undefined);
    expect(result.error).toBeInstanceOf(SerializationError);
    expect(result.error.path).toMatch(/nested.*invalid/);
  });

  it("rejects invalid stash data before recording recovery progress", async () => {
    const wf = workflow(async (ctx) =>
      ctx.step(async ({ stash }) => {
        await stash({ cursor: 1n });
        return "unreachable";
      }, { name: "stash-invalid" })
    );
    const result = await testEngine().run(wf, undefined);
    expect(result.steps[0]?.error).toBeInstanceOf(SerializationError);
    expect(result.steps[0]?.stashed).toBeUndefined();
  });

  it("normalizes array holes to null so live and resumed values agree", async () => {
    const wf = workflow(async (ctx) =>
      ctx.step(() => Promise.resolve([1, , 3] as unknown as number[]), { name: "sparse" })
    );
    const te = testEngine();

    const crashed = await te.run(wf, undefined, { crashAfter: "sparse" });
    expect(crashed.steps[0]?.output).toEqual([1, null, 3]);
    const done = await te.resume(crashed.runId);

    expect(done.result).toEqual([1, null, 3]);
  });

  it("drops an undefined property the way JSON.stringify does", async () => {
    const wf = workflow(async (ctx) =>
      ctx.step(() => Promise.resolve({ id: "x", nickname: undefined }), { name: "optional-field" })
    );

    const result = await testEngine().run(wf, undefined);
    expect(result.status).toBe("completed");
    expect(result.steps[0]?.output).toEqual({ id: "x" });
  });

  it("rejects non-serializable loop snapshots at the iteration that produced them", async () => {
    const wf = workflow(async (ctx) =>
      ctx.loop({ turn: 0, value: "ok" as unknown }, async (state) => ({
        turn: state.turn + 1,
        value: state.turn === 0 ? new Map([["x", 1]]) : state.value,
      }), { snapshotEvery: 1, maxIterations: 2 })
    );
    const result = await testEngine().run(wf, undefined);
    expect(result.error).toBeInstanceOf(SerializationError);
    expect(result.error.path).toMatch(/value/);
  });
});
