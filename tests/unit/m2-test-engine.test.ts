import { describe, expect, it } from "bun:test";
import { PurityError, testEngine, workflow } from "../../index.ts";

const threeStepWorkflow = (executions: string[] = []) =>
  workflow(async (ctx) => {
    for (const name of ["fetch", "generate", "publish"]) {
      await ctx.step(() => {
        executions.push(name);
        return Promise.resolve(name);
      }, { name });
    }
    return executions.join(",");
  });

describe("M2 testEngine: deterministic failure injection", () => {
  it("reports completed, crashed, failed, and waiting as distinct statuses", async () => {
    const complete = workflow(async (ctx) => ctx.step(() => Promise.resolve("ok"), { name: "ok" }));
    const fail = workflow(async (ctx) =>
      ctx.step(() => Promise.reject(new Error("boom")), { name: "boom" })
    );
    const wait = workflow(async (ctx) => ctx.waitFor("approval"));

    expect((await testEngine().run(complete, undefined)).status).toBe("completed");
    expect((await testEngine().run(complete, undefined, { crashAfter: "ok" })).status).toBe("crashed");
    expect((await testEngine().run(fail, undefined)).status).toBe("failed");
    expect((await testEngine().run(wait, undefined)).status).toBe("waiting");
  });

  it("crashAfter accepts a canonical hierarchical path", async () => {
    const wf = threeStepWorkflow();
    const result = await testEngine().run(wf, undefined, { crashAfter: [1] });
    expect(result.status).toBe("crashed");
    expect(result.steps.map((step) => step.label)).toEqual(["fetch", "generate"]);
  });

  it("crashAfter label selects the first matching label", async () => {
    const wf = workflow(async (ctx) => {
      await ctx.step(() => Promise.resolve(1), { name: "repeat" });
      await ctx.step(() => Promise.resolve(2), { name: "repeat" });
      await ctx.step(() => Promise.resolve(3), { name: "after" });
    });

    const result = await testEngine().run(wf, undefined, { crashAfter: "repeat" });
    expect(result.steps).toHaveLength(1);
    expect(result.steps[0]?.output).toBe(1);
  });

  it("the nth selector disambiguates duplicate labels", async () => {
    const wf = workflow(async (ctx) => {
      await ctx.step(() => Promise.resolve(1), { name: "repeat" });
      await ctx.step(() => Promise.resolve(2), { name: "repeat" });
      await ctx.step(() => Promise.resolve(3), { name: "repeat" });
    });

    const result = await testEngine().run(wf, undefined, {
      crashAfter: { label: "repeat", nth: 2 },
    });
    expect(result.steps.map((step) => step.output)).toEqual([1, 2]);
  });

  it("crashInStep interrupts an effect before it records completion", async () => {
    let calls = 0;
    const wf = workflow(async (ctx) =>
      ctx.step(async ({ stash }) => {
        calls++;
        await stash({ cursor: 10 });
        return "complete";
      }, { name: "stream" })
    );
    const te = testEngine();

    const crashed = await te.run(wf, undefined, { crashInStep: "stream" });
    expect(crashed.status).toBe("crashed");
    expect(crashed.steps[0]).toEqual(expect.objectContaining({
      label: "stream",
      executions: 1,
      stashed: { cursor: 10 },
    }));

    const resumed = await te.resume(crashed.runId);
    expect(resumed.result).toBe("complete");
    expect(resumed.steps[0]?.executions).toBe(2);
    expect(calls).toBe(2);
  });

  it("resume rejects an unknown run id", async () => {
    await expect(testEngine().resume("run-does-not-exist")).rejects.toThrow(/not found|unknown/i);
  });

  it("shadow replay runs automatically without increasing executions", async () => {
    const calls: string[] = [];
    const result = await testEngine().run(threeStepWorkflow(calls), undefined);

    expect(result.steps.every((step) => step.executions === 1)).toBe(true);
    expect(calls).toEqual(["fetch", "generate", "publish"]);
  });

  it("shadow replay detects impure control flow in an otherwise passing workflow", async () => {
    let replayCount = 0;
    const impure = workflow(async (ctx) => {
      replayCount++;
      if (replayCount === 1) {
        await ctx.step(() => Promise.resolve("conditional"), { name: "conditional" });
      }
      return ctx.step(() => Promise.resolve("last"), { name: "last" });
    });

    await expect(testEngine().run(impure, undefined)).rejects.toBeInstanceOf(PurityError);
  });

  it("fake clock advances durable sleep without waiting in wall-clock time", async () => {
    const te = testEngine();
    const wf = workflow(async (ctx) => {
      await ctx.sleep(60_000);
      return "awake";
    });
    const startedAt = performance.now();

    const sleeping = await te.run(wf, undefined);
    expect(sleeping.status).toBe("sleeping");
    te.clock.advance(59_999);
    expect((await te.resume(sleeping.runId)).status).toBe("sleeping");
    te.clock.advance(1);
    const awake = await te.resume(sleeping.runId);

    expect(awake.result).toBe("awake");
    expect(performance.now() - startedAt).toBeLessThan(1_000);
  });

  it("signal wakes a waiting run with its payload", async () => {
    const te = testEngine();
    const wf = workflow(async (ctx) => {
      const approval = await ctx.waitFor("approval");
      return { approval };
    });

    const waiting = await te.run(wf, undefined);
    await te.signal(waiting.runId, "approval", { by: "nikhil" });
    const done = await te.resume(waiting.runId);

    expect(done.result).toEqual({ approval: { by: "nikhil" } });
  });

  it("a signal with the wrong name does not wake the run", async () => {
    const te = testEngine();
    const wf = workflow(async (ctx) => ctx.waitFor("approval"));
    const waiting = await te.run(wf, undefined);

    await te.signal(waiting.runId, "unrelated", { ignored: true });
    expect((await te.resume(waiting.runId)).status).toBe("waiting");
  });
});
