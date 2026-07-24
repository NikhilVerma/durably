import { describe, expect, it } from "bun:test";
import {
  BudgetExceededError,
  CircuitOpenError,
  MemoryStorage,
  createEngine,
  resource,
  workflow,
} from "../../index.ts";
import { SqliteStorage } from "../../sqlite.ts";
import { deferred, tick, withTempDir } from "../support/kit.ts";

describe("M7 shared resources", () => {
  it("enforces resource concurrency across separate runs", async () => {
    const gates = Array.from({ length: 4 }, () => deferred<void>());
    let active = 0;
    let peak = 0;
    let started = 0;
    const api = resource("api", { concurrency: 2 });
    const engine = createEngine({
      storage: new MemoryStorage(),
      resources: [api],
      concurrency: 4,
    });
    const wf = workflow<{ index: number }>(async (ctx, { index }) =>
      ctx.step(async () => {
        active++;
        started++;
        peak = Math.max(peak, active);
        await gates[index]!.promise;
        active--;
        return index;
      }, { name: "call-api", uses: "api" })
    );
    for (let index = 0; index < 4; index++) await engine.enqueue(wf, { index });
    await engine.start();
    await tick();

    expect(started).toBe(2);
    gates[0]!.resolve();
    gates[1]!.resolve();
    await tick();
    expect(started).toBe(4);
    gates[2]!.resolve();
    gates[3]!.resolve();
    await engine.drain();

    expect(peak).toBe(2);
    await engine.stop();
  });

  it("applies one token bucket across all runs using the same resource", async () => {
    const starts: number[] = [];
    const api = resource("rate-limited", {
      rateLimit: { max: 2, perMs: 1 },
    });
    const engine = createEngine({
      storage: new MemoryStorage(),
      resources: [api],
      concurrency: 3,
    });
    const wf = workflow(async (ctx) =>
      ctx.step(() => {
        starts.push(performance.now());
        return Promise.resolve();
      }, { name: "call-api", uses: "rate-limited" })
    );
    await engine.enqueue(wf, undefined);
    await engine.enqueue(wf, undefined);
    await engine.enqueue(wf, undefined);
    await engine.start();
    await engine.drain();

    expect(starts).toHaveLength(3);
    expect(starts[2]! - starts[0]!).toBeGreaterThanOrEqual(0.5);
    await engine.stop();
  });

  it("eventually opens, then half-opens and closes without assuming a minimum sample count", async () => {
    let providerCalls = 0;
    let shouldFail = true;
    const provider = resource("provider", {
      breaker: {
        failureRate: 0.5,
        windowMs: 1_000,
        cooldownMs: 1,
      },
    });
    const engine = createEngine({ storage: new MemoryStorage(), resources: [provider], concurrency: 1 });
    const failing = workflow<number>(async (ctx) =>
      ctx.step(() => {
        providerCalls++;
        if (shouldFail) return Promise.reject(new Error("provider down"));
        return Promise.resolve("up");
      }, { name: "provider-call", uses: "provider" })
    );
    await engine.start();
    let blockedState: Awaited<ReturnType<typeof engine.inspect>> | undefined;
    for (let input = 0; input < 100 && !blockedState; input++) {
      const handle = await engine.enqueue(failing, input);
      await engine.drain();
      const state = await engine.inspect(handle.runId);
      if (state?.error instanceof CircuitOpenError) blockedState = state;
    }
    expect(blockedState?.error).toEqual(expect.objectContaining({
      resource: "provider",
    }));
    shouldFail = false;
    await new Promise((resolve) => setTimeout(resolve, 2));
    const probe = await engine.enqueue(failing, 101);
    await engine.drain();

    expect((await engine.inspect(probe.runId))?.result).toBe("up");
    const after = await engine.enqueue(failing, 102);
    await engine.drain();
    expect((await engine.inspect(after.runId))?.result).toBe("up");
    expect(providerCalls).toBeGreaterThan(1);
    await engine.stop();
  });

  it("enforces the engine-wide budget across distinct runs", async () => {
    const engine = createEngine({
      storage: new MemoryStorage(),
      budget: { usd: 1 },
      concurrency: 1,
    });
    const wf = workflow<number>(async (ctx) => {
      ctx.charge({ usd: 0.6 });
      return "charged";
    });
    await engine.start();
    const first = await engine.enqueue(wf, 1);
    const second = await engine.enqueue(wf, 2);
    await engine.drain();

    expect((await engine.inspect(first.runId))?.status).toBe("completed");
    expect((await engine.inspect(second.runId))?.status).toBe("failed");
    expect((await engine.inspect(second.runId))?.error).toBeInstanceOf(BudgetExceededError);
    await engine.stop();
  });
});

describe("M7 SqliteStorage adapter", () => {
  it("persists and resumes a waiting run through a new engine instance", async () => {
    await withTempDir(async (dir) => {
      const database = `${dir}/durably.sqlite`;
      let executions = 0;
      const wf = workflow(async (ctx) => {
        await ctx.step(() => Promise.resolve(++executions), { name: "once" });
        return ctx.waitFor("approval");
      });

      const first = createEngine({ storage: new SqliteStorage(database) });
      await first.start();
      const handle = await first.enqueue(wf, undefined);
      await first.drain();
      await first.stop();

      const second = createEngine({ storage: new SqliteStorage(database) });
      await second.start();
      await second.signal(handle.runId, "approval", { by: "operator" });
      await second.drain();

      expect(executions).toBe(1);
      expect(await second.inspect(handle.runId)).toEqual(expect.objectContaining({
        status: "completed",
        result: { by: "operator" },
      }));
      await second.stop();
    });
  });

  it("persists an open circuit across engine restart", async () => {
    await withTempDir(async (dir) => {
      const database = `${dir}/durably.sqlite`;
      const provider = resource("persistent-provider", {
        breaker: { failureRate: 0.5, windowMs: 1_000, cooldownMs: 10_000 },
      });
      let calls = 0;
      const wf = workflow<number>(async (ctx) =>
        ctx.step(() => {
          calls++;
          throw new Error("down");
        }, { name: "provider-call", uses: "persistent-provider" })
      );

      const first = createEngine({
        storage: new SqliteStorage(database),
        resources: [provider],
        concurrency: 1,
      });
      await first.start();
      let opened = false;
      for (let input = 0; input < 100 && !opened; input++) {
        const handle = await first.enqueue(wf, input);
        await first.drain();
        opened = (await first.inspect(handle.runId))?.error instanceof CircuitOpenError;
      }
      expect(opened).toBe(true);
      const callsAtOpen = calls;
      await first.stop();

      const second = createEngine({
        storage: new SqliteStorage(database),
        resources: [provider],
      });
      await second.start();
      const blocked = await second.enqueue(wf, 101);
      await second.drain();

      expect((await second.inspect(blocked.runId))?.error).toBeInstanceOf(CircuitOpenError);
      expect(calls).toBe(callsAtOpen);
      await second.stop();
    });
  });
});
