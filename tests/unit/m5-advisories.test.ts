import { describe, expect, it, spyOn } from "bun:test";
import {
  BudgetExceededError,
  CircuitOpenError,
  KeyConflictError,
  LeaseLostError,
  PurityError,
  RunCancelledError,
  SerializationError,
  StaleRunError,
  StepTimeoutError,
  ValidationError,
  run,
  testEngine,
  workflow,
  type Advisory,
} from "../../index.ts";
import { deferred, findFilesNamed, readJson, tick, withTempDir } from "../support/kit.ts";

const byCode = (advisories: readonly Advisory[], code: Advisory["code"]): Advisory => {
  const advisory = advisories.find((item) => item.code === code);
  expect(advisory).toBeDefined();
  return advisory!;
};

const expectUsefulAdvisory = (advisory: Advisory) => {
  expect(advisory).toEqual({
    code: expect.any(String),
    level: expect.stringMatching(/^(info|warn)$/),
    msg: expect.any(String),
    hint: expect.any(String),
    docs: expect.any(String),
    count: expect.any(Number),
    firstAt: expect.any(String),
  });
  expect(advisory.msg.trim().length).toBeGreaterThan(10);
  expect(advisory.hint.trim().length).toBeGreaterThan(10);
  expect(advisory.docs).toMatch(/^(https?:\/\/|#|durably\.dev\/)/);
};

describe("M5 advisories: initial code registry", () => {
  it("emits LOOP_SUGGESTED at info 1k and escalates once at warn 5k", async () => {
    const wf = workflow(async (ctx) => {
      for (let index = 0; index < 5_000; index++) {
        await ctx.step(() => Promise.resolve(index), { name: "loop-body" });
      }
      return "done";
    });
    const te = testEngine();

    const atInfo = await te.run(wf, undefined, {
      crashAfter: { label: "loop-body", nth: 1_000 },
    });
    const info = byCode(atInfo.advisories, "LOOP_SUGGESTED");
    expect(info).toEqual(expect.objectContaining({ level: "info", count: 1 }));
    const originalMessage = info.msg;

    const atWarn = await te.resume(atInfo.runId);
    const warning = byCode(atWarn.advisories, "LOOP_SUGGESTED");
    expect(warning).toEqual(expect.objectContaining({
      level: "warn",
      count: 2,
      msg: originalMessage,
      firstAt: info.firstAt,
    }));
    expect(atWarn.advisories.filter((item) => item.code === "LOOP_SUGGESTED")).toHaveLength(1);
  }, 20_000);

  it("emits FANOUT_CEILING at 1k children and escalates at 5k", async () => {
    const child = workflow<number>()(async (_ctx, value) => value);
    const firstBatch = Array.from({ length: 1_000 }, (_, index) => [child, index] as const);
    const secondBatch = Array.from({ length: 4_000 }, (_, index) => [child, index + 1_000] as const);
    const parent = workflow(async (ctx) => {
      await ctx.spawnAll(firstBatch);
      await ctx.step(() => Promise.resolve(), { name: "after-1k" });
      await ctx.spawnAll(secondBatch);
      return "done";
    });
    const te = testEngine();

    const atInfo = await te.run(parent, undefined, { crashAfter: "after-1k" });
    expect(byCode(atInfo.advisories, "FANOUT_CEILING")).toEqual(expect.objectContaining({
      level: "info",
      count: 1,
    }));
    const atWarn = await te.resume(atInfo.runId);
    expect(byCode(atWarn.advisories, "FANOUT_CEILING")).toEqual(expect.objectContaining({
      level: "warn",
      count: 2,
      hint: expect.stringMatching(/batch/i),
    }));
  }, 30_000);

  it("emits RETRY_STORM for straight failures of the same class on one step", async () => {
    const wf = workflow(async (ctx) =>
      ctx.step(() => {
        throw new TypeError("provider quota");
      }, {
        name: "provider-call",
        retry: { attempts: 20, backoff: "none" },
      })
    );

    const result = await testEngine().run(wf, undefined);
    const advisory = byCode(result.advisories, "RETRY_STORM");
    expect(advisory).toEqual(expect.objectContaining({
      level: "info",
      hint: expect.stringMatching(/resource|breaker/i),
    }));
  });

  it("does not call mixed error classes a RETRY_STORM", async () => {
    let attempt = 0;
    const wf = workflow(async (ctx) =>
      ctx.step(() => {
        attempt++;
        throw attempt % 2 === 0 ? new TypeError("type") : new RangeError("range");
      }, {
        name: "mixed-errors",
        retry: { attempts: 20, backoff: "none" },
      })
    );

    const result = await testEngine().run(wf, undefined);
    expect(result.advisories.some((item) => item.code === "RETRY_STORM")).toBe(false);
  });

  it("emits BUDGET_NEAR on crossing 80 percent, once per run", async () => {
    const wf = workflow(async (ctx) => {
      ctx.charge({ usd: 0.79 });
      ctx.charge({ usd: 0.01 });
      ctx.charge({ usd: 0.1 });
      return "done";
    });

    const result = await testEngine().run(wf, undefined, { budget: { usd: 1 } });
    expect(byCode(result.advisories, "BUDGET_NEAR")).toEqual(expect.objectContaining({
      count: 1,
      hint: expect.stringMatching(/budget|remaining/i),
    }));
  });

  it("emits WAITFOR_NO_TIMEOUT when a run parks indefinitely", async () => {
    const wf = workflow(async (ctx) => ctx.waitFor("approval"));

    const result = await testEngine().run(wf, undefined);

    expect(result.status).toBe("waiting");
    expect(byCode(result.advisories, "WAITFOR_NO_TIMEOUT")).toEqual(expect.objectContaining({
      level: "info",
      hint: expect.stringMatching(/timeoutMs|timeout/i),
    }));
  });

  it("does not emit WAITFOR_NO_TIMEOUT when timeoutMs is supplied", async () => {
    const wf = workflow(async (ctx) =>
      ctx.waitFor("approval", undefined, { timeoutMs: 60_000 })
    );

    const result = await testEngine().run(wf, undefined);
    expect(result.advisories.some((item) => item.code === "WAITFOR_NO_TIMEOUT")).toBe(false);
  });

  it("emits SNAPSHOT_HEAVY when state size times snapshot frequency is excessive", async () => {
    const payload = "x".repeat(1_000_000);
    const wf = workflow(async (ctx) =>
      ctx.loop({ iteration: 0, payload }, async (state, { done }) => {
        if (state.iteration === 9) return done("done");
        return { ...state, iteration: state.iteration + 1 };
      }, { snapshotEvery: 1 })
    );

    const result = await testEngine().run(wf, undefined);
    expect(byCode(result.advisories, "SNAPSHOT_HEAVY")).toEqual(expect.objectContaining({
      hint: expect.stringMatching(/snapshotEvery|compact|state/i),
    }));
  });

  it("emits SLOW_STEP_NO_TIMEOUT after a 30s+ attempt without timeoutMs", async () => {
    const gate = deferred<void>();
    const te = testEngine();
    const wf = workflow(async (ctx) =>
      ctx.step(async () => {
        await gate.promise;
        return "done";
      }, { name: "slow" })
    );

    const pending = te.run(wf, undefined);
    await tick();
    te.clock.advance(30_001);
    gate.resolve();
    const result = await pending;

    expect(byCode(result.advisories, "SLOW_STEP_NO_TIMEOUT")).toEqual(expect.objectContaining({
      level: "info",
      hint: expect.stringMatching(/timeoutMs|timeout/i),
    }));
    expect(result.steps[0]).toEqual(expect.objectContaining({
      startedAt: expect.any(String),
      ms: 30_001,
    }));
  });

  it("does not emit SLOW_STEP_NO_TIMEOUT when a timeout policy exists", async () => {
    const wf = workflow(async (ctx) =>
      ctx.step(() => Promise.resolve("fast"), { name: "bounded", timeoutMs: 60_000 })
    );
    const result = await testEngine().run(wf, undefined);
    expect(result.advisories.some((item) => item.code === "SLOW_STEP_NO_TIMEOUT")).toBe(false);
  });

  it("emits STASH_SUGGESTED only when a slow first attempt re-runs from zero", async () => {
    const gate = deferred<void>();
    let calls = 0;
    const te = testEngine();
    const wf = workflow(async (ctx) =>
      ctx.step(async () => {
        calls++;
        if (calls === 1) await gate.promise;
        return "done";
      }, { name: "recoverable-stream" })
    );

    const pendingCrash = te.run(wf, undefined, { crashInStep: "recoverable-stream" });
    await tick();
    te.clock.advance(30_001);
    gate.resolve();
    const crashed = await pendingCrash;
    const done = await te.resume(crashed.runId);

    expect(byCode(done.advisories, "STASH_SUGGESTED")).toEqual(expect.objectContaining({
      level: "info",
      hint: expect.stringMatching(/stash|stashed/i),
    }));
    expect(done.steps[0]).toEqual(expect.objectContaining({
      executions: 2,
      attemptHistory: [
        expect.objectContaining({
          attempt: 1,
          startedAt: expect.any(String),
          ms: 30_001,
          stashed: false,
        }),
        expect.objectContaining({
          attempt: 2,
          startedAt: expect.any(String),
          ms: 0,
        }),
      ],
    }));
  });

  it("does not emit STASH_SUGGESTED when progress was stashed", async () => {
    const gate = deferred<void>();
    let calls = 0;
    const te = testEngine();
    const wf = workflow(async (ctx) =>
      ctx.step(async ({ stash }) => {
        calls++;
        if (calls === 1) {
          await stash({ offset: 100 });
          await gate.promise;
        }
        return "done";
      }, { name: "stashed-stream" })
    );

    const pendingCrash = te.run(wf, undefined, { crashInStep: "stashed-stream" });
    await tick();
    te.clock.advance(30_001);
    gate.resolve();
    const crashed = await pendingCrash;
    const done = await te.resume(crashed.runId);

    expect(done.advisories.some((item) => item.code === "STASH_SUGGESTED")).toBe(false);
  });
});

describe("M5 advisories: anti-spam and propagation", () => {
  it("keeps run(): T while delivering each advisory through onAdvisory", async () => {
    await withTempDir(async (dir) => {
      const observed: Advisory[] = [];
      const wf = workflow(async (ctx) => {
        ctx.charge({ usd: 0.8 });
        return 42;
      });

      const value = await run(wf, undefined, {
        dir,
        budget: { usd: 1 },
        onAdvisory: (advisory) => observed.push(advisory),
      });

      expect(value).toBe(42);
      expect(observed).toEqual([
        expect.objectContaining({ code: "BUDGET_NEAR", count: 1 }),
      ]);
    });
  });

  it("run onAdvisory emits once per severity crossing while stderr stays once per code", async () => {
    await withTempDir(async (dir) => {
      const observed: Advisory[] = [];
      const stderr = spyOn(process.stderr, "write").mockImplementation(() => true);
      try {
        const wf = workflow(async (ctx) => {
          for (let index = 0; index < 5_000; index++) {
            await ctx.step(() => Promise.resolve(index), { name: "loop-body" });
          }
          return "done";
        });
        expect(await run(wf, undefined, {
          dir,
          onAdvisory: (advisory) => observed.push(advisory),
        })).toBe("done");

        expect(observed.filter((item) => item.code === "LOOP_SUGGESTED").map((item) => item.level))
          .toEqual(["info", "warn"]);
        const writes = stderr.mock.calls.flatMap((call) => call).map(String)
          .filter((line) => line.includes("LOOP_SUGGESTED"));
        expect(writes).toHaveLength(1);
        const [statePath] = await findFilesNamed(dir, "state.json");
        const state = await readJson<{ runId: string }>(statePath!);
        expect(state.runId).toEqual(expect.any(String));
        expect(writes[0]).toContain(state.runId);
      } finally {
        stderr.mockRestore();
      }
    });
  }, 30_000);

  it("attaches runId and all active advisories to a thrown DurablyError", async () => {
    await withTempDir(async (dir) => {
      const stderr = spyOn(process.stderr, "write").mockImplementation(() => true);
      const wf = workflow(async (ctx) => {
        ctx.charge({ usd: 0.8 });
        ctx.charge({ usd: 0.3 });
        return "unreachable";
      });

      try {
        await run(wf, undefined, { dir, budget: { usd: 1 } });
        throw new Error("expected run to fail");
      } catch (error) {
        expect(error).toBeInstanceOf(BudgetExceededError);
        expect(error).toEqual(expect.objectContaining({
          runId: expect.any(String),
          advisories: [
            expect.objectContaining({ code: "BUDGET_NEAR" }),
          ],
        }));
        const lines = stderr.mock.calls.flatMap((call) => call).map(String)
          .filter((line) => line.includes("BUDGET_NEAR"));
        expect(lines).toHaveLength(1);
        expect(lines[0]).toContain((error as BudgetExceededError).runId);
      } finally {
        stderr.mockRestore();
      }
    });
  });

  it("attaches active advisories to a representative non-budget DurablyError", async () => {
    await withTempDir(async (dir) => {
      const invalidSchema = {
        "~standard": {
          version: 1 as const,
          vendor: "test",
          validate: () => ({ issues: [{ message: "invalid extraction" }] }),
        },
      };
      const wf = workflow(async (ctx) => {
        ctx.charge({ usd: 0.8 });
        return ctx.step(() => Promise.resolve("bad"), {
          name: "validated",
          schema: invalidSchema,
        });
      });
      try {
        await run(wf, undefined, { dir, budget: { usd: 1 } });
        throw new Error("expected ValidationError");
      } catch (error) {
        expect(error).toBeInstanceOf(ValidationError);
        expect(error).toEqual(expect.objectContaining({
          runId: expect.any(String),
          advisories: [expect.objectContaining({ code: "BUDGET_NEAR" })],
        }));
      }
    });
  });

  it("silent run mode suppresses onAdvisory and stderr but preserves T", async () => {
    await withTempDir(async (dir) => {
      const observed: Advisory[] = [];
      const stderr = spyOn(process.stderr, "write").mockImplementation(() => true);
      const wf = workflow(async (ctx) => {
        ctx.charge({ usd: 0.9 });
        return "ok" as const;
      });

      const value = await run(wf, undefined, {
        dir,
        budget: { usd: 1 },
        advisories: "silent",
        onAdvisory: (advisory) => observed.push(advisory),
      });

      expect(value).toBe("ok");
      expect(observed).toEqual([]);
      expect(stderr).not.toHaveBeenCalled();
      stderr.mockRestore();
    });
  });

  it("each advisory uses the documented fields and the active set never exceeds ten", async () => {
    const wf = workflow(async (ctx) => {
      ctx.charge({ usd: 0.8 });
      for (let index = 0; index < 1_000; index++) {
        await ctx.step(() => Promise.resolve(index), { name: "loop-body" });
      }
      return ctx.waitFor("approval");
    });
    const result = await testEngine().run(wf, undefined, { budget: { usd: 1 } });

    expect(result.advisories.length).toBeGreaterThanOrEqual(3);
    expect(result.advisories.length).toBeLessThanOrEqual(10);
    for (const advisory of result.advisories) expectUsefulAdvisory(advisory);
    expect(new Set(result.advisories.map((item) => item.code)).size).toBe(result.advisories.length);
  });

  it("silent mode suppresses collection and stderr without changing execution", async () => {
    const stderr = spyOn(process.stderr, "write").mockImplementation(() => true);
    const wf = workflow(async (ctx) => {
      ctx.charge({ usd: 0.9 });
      return ctx.waitFor("approval");
    });

    const result = await testEngine().run(wf, undefined, {
      budget: { usd: 1 },
      advisories: "silent",
    });

    expect(result.status).toBe("waiting");
    expect(result.advisories).toEqual([]);
    expect(stderr).not.toHaveBeenCalled();
    stderr.mockRestore();
  });

  it("writes stderr only once per advisory code, including after severity escalation", async () => {
    const stderr = spyOn(process.stderr, "write").mockImplementation(() => true);
    const wf = workflow(async (ctx) => {
      for (let index = 0; index < 5_000; index++) {
        await ctx.step(() => Promise.resolve(index), { name: "loop-body" });
      }
    });
    const te = testEngine();
    const first = await te.run(wf, undefined, {
      crashAfter: { label: "loop-body", nth: 1_000 },
    });
    await te.resume(first.runId);

    const loopWrites = stderr.mock.calls
      .flatMap((call) => call)
      .map(String)
      .filter((text) => text.includes("LOOP_SUGGESTED"));
    expect(loopWrites).toHaveLength(1);
    stderr.mockRestore();
  }, 20_000);

  it("reserves warn for an approaching quantitative ceiling", async () => {
    const wf = workflow(async (ctx) => {
      for (let index = 0; index < 5_000; index++) {
        await ctx.step(() => Promise.resolve(index), { name: "loop-body" });
      }
      await ctx.step(() => Promise.reject(new Error("failure")), {
        name: "retry",
        retry: { attempts: 20, backoff: "none" },
      });
    });
    const result = await testEngine().run(wf, undefined);

    expect(result.advisories.some((advisory) => advisory.level === "warn")).toBe(true);
    for (const advisory of result.advisories) {
      if (advisory.level === "warn") {
        expect([
          "LOOP_SUGGESTED",
          "FANOUT_CEILING",
          "BUDGET_NEAR",
          "SNAPSHOT_HEAVY",
        ]).toContain(advisory.code);
      }
    }
  }, 20_000);
});

describe("M5 DurablyError self-correction hints", () => {
  it.each([
    ValidationError,
    SerializationError,
    StepTimeoutError,
    PurityError,
    StaleRunError,
    BudgetExceededError,
    CircuitOpenError,
    KeyConflictError,
    RunCancelledError,
    LeaseLostError,
  ])("%s carries a nonempty hint and docs location", (ErrorClass) => {
    const error = new ErrorClass("test");
    expect(error.hint).toEqual(expect.any(String));
    expect(error.hint.length).toBeGreaterThan(10);
    expect(error.docs).toMatch(/^(https?:\/\/|#|durably\.dev\/)/);
    expect(error).toHaveProperty("runId");
    expect(error.advisories).toEqual(expect.any(Array));
  });

  it("StaleRunError names both implemented recovery paths", () => {
    const error = new StaleRunError("run-1");
    expect(error.hint).toMatch(/restart/);
    expect(error.hint).toMatch(/adopt/);
    expect(error.hint).toMatch(/compatib|assert/i);
  });

  it("hints mention only APIs present in the public contract", () => {
    const publicMoves = [
      "ctx", "durably",
      "step", "loop", "stash", "timeoutMs", "retry", "resource", "breaker",
      "budget", "waitFor", "snapshotEvery", "restart", "adopt", "fresh",
      "SqliteStorage", "FileStorage", "inspect",
    ];
    const errors = [
      new ValidationError("test"),
      new SerializationError("test"),
      new StepTimeoutError("test"),
      new PurityError("test"),
      new StaleRunError("test"),
      new BudgetExceededError("test"),
      new CircuitOpenError("test"),
      new KeyConflictError("test"),
      new RunCancelledError("test"),
      new LeaseLostError("test"),
    ];

    for (const error of errors) {
      const codeLikeWords = error.hint.match(/\b[A-Za-z_$][\w$]*(?=\(|\.)/g) ?? [];
      for (const word of codeLikeWords) expect(publicMoves).toContain(word);
    }
  });
});
