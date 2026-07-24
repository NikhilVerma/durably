import { describe, expect, it } from "bun:test";
import {
  BudgetExceededError,
  StepTimeoutError,
  ValidationError,
  testEngine,
  workflow,
} from "../../index.ts";
import { deferred, numberSchema, tick } from "../support/kit.ts";

describe("M3 policies: retry and timeout", () => {
  it("does not retry unless a retry policy is present", async () => {
    let attempts = 0;
    const wf = workflow(async (ctx) =>
      ctx.step(() => {
        attempts++;
        throw new Error("permanent");
      }, { name: "single-attempt" })
    );

    const result = await testEngine().run(wf, undefined);
    expect(result.status).toBe("failed");
    expect(attempts).toBe(1);
    expect(result.steps[0]?.attempts).toBe(1);
  });

  it("counts attempts as the total including the initial call", async () => {
    let attempts = 0;
    const wf = workflow(async (ctx) =>
      ctx.step(() => {
        attempts++;
        if (attempts < 3) throw new Error(`flaky ${attempts}`);
        return Promise.resolve("ok");
      }, {
        name: "flaky",
        retry: { attempts: 3, backoff: "none" },
      })
    );

    const result = await testEngine().run(wf, undefined);
    expect(result.result).toBe("ok");
    expect(attempts).toBe(3);
    expect(result.steps[0]?.attempts).toBe(3);
  });

  it("stops after the configured retry attempt ceiling", async () => {
    let attempts = 0;
    const wf = workflow(async (ctx) =>
      ctx.step(() => {
        attempts++;
        throw new Error("still broken");
      }, {
        name: "bounded",
        retry: { attempts: 4, backoff: "none" },
      })
    );

    const result = await testEngine().run(wf, undefined);
    expect(result.status).toBe("failed");
    expect(attempts).toBe(4);
    expect(result.steps[0]?.error).toEqual(expect.objectContaining({ message: "still broken" }));
  });

  it("uses linear backoff on the fake clock", async () => {
    let attempts = 0;
    const te = testEngine();
    const wf = workflow(async (ctx) =>
      ctx.step(() => {
        attempts++;
        if (attempts < 4) throw new Error("retry");
        return Promise.resolve(attempts);
      }, {
        name: "linear",
        retry: { attempts: 4, backoff: "linear", baseMs: 100 },
      })
    );

    const pending = te.run(wf, undefined);
    await tick();
    expect(attempts).toBe(1);
    te.clock.advance(99);
    await tick();
    expect(attempts).toBe(1);
    te.clock.advance(1);
    await tick();
    expect(attempts).toBe(2);
    te.clock.advance(199);
    await tick();
    expect(attempts).toBe(2);
    te.clock.advance(1);
    await tick();
    expect(attempts).toBe(3);
    te.clock.advance(300);

    expect((await pending).result).toBe(4);
  });

  it("uses capped exponential backoff on the fake clock", async () => {
    let attempts = 0;
    const te = testEngine();
    const wf = workflow(async (ctx) =>
      ctx.step(() => {
        attempts++;
        if (attempts < 5) throw new Error("retry");
        return Promise.resolve("ok");
      }, {
        name: "exponential",
        retry: {
          attempts: 5,
          backoff: "exponential",
          baseMs: 100,
          maxMs: 250,
        },
      })
    );

    const pending = te.run(wf, undefined);
    await tick();
    for (const delay of [100, 200, 250, 250]) {
      te.clock.advance(delay);
      await tick();
    }
    expect((await pending).result).toBe("ok");
    expect(attempts).toBe(5);
  });

  it("aborts the attempt signal at timeout and reports StepTimeoutError", async () => {
    let observedAbort = false;
    const te = testEngine();
    const wf = workflow(async (ctx) =>
      ctx.step(({ signal }) =>
        new Promise<never>((_resolve, reject) => {
          signal.addEventListener("abort", () => {
            observedAbort = true;
            reject(signal.reason);
          }, { once: true });
        }), { name: "hung", timeoutMs: 500 })
    );

    const pending = te.run(wf, undefined);
    await tick();
    te.clock.advance(499);
    await tick();
    expect(observedAbort).toBe(false);
    te.clock.advance(1);
    const result = await pending;

    expect(observedAbort).toBe(true);
    expect(result.status).toBe("failed");
    expect(result.steps[0]?.error).toBeInstanceOf(StepTimeoutError);
    expect(result.steps[0]?.error).toEqual(expect.objectContaining({ timeoutMs: 500 }));
  });

  it("creates a fresh AbortSignal for every retry attempt", async () => {
    const signals: AbortSignal[] = [];
    const te = testEngine();
    const wf = workflow(async (ctx) =>
      ctx.step(({ signal }) => {
        signals.push(signal);
        if (signals.length === 1) throw new Error("retry");
        return Promise.resolve("ok");
      }, {
        name: "fresh-signal",
        retry: { attempts: 2, backoff: "none" },
      })
    );

    expect((await te.run(wf, undefined)).result).toBe("ok");
    expect(signals).toHaveLength(2);
    expect(signals[0]).not.toBe(signals[1]);
  });
});

describe("M3 policies: validation, compensation, concurrency, and budget", () => {
  it("accepts a Standard Schema output and returns its parsed value", async () => {
    const coercingNumberSchema = {
      "~standard": {
        version: 1 as const,
        vendor: "durably-test",
        validate: (value: unknown) =>
          typeof value === "string" && value.trim() !== "" && Number.isFinite(Number(value))
            ? { value: Number(value) }
            : { issues: [{ message: "expected a numeric string" }] },
      },
    };
    const wf = workflow(async (ctx) =>
      ctx.step(() => Promise.resolve("42"), {
        name: "extract",
        schema: coercingNumberSchema,
      })
    );

    expect((await testEngine().run(wf, undefined)).result).toBe(42);
  });

  it("fails at the producing step with Standard Schema issues", async () => {
    const wf = workflow(async (ctx) =>
      ctx.step(() => Promise.resolve("not a number"), {
        name: "extract",
        schema: numberSchema,
      })
    );

    const result = await testEngine().run(wf, undefined);
    expect(result.status).toBe("failed");
    expect(result.steps[0]?.error).toBeInstanceOf(ValidationError);
    expect(result.steps[0]?.error).toEqual(expect.objectContaining({
      issues: [expect.objectContaining({ message: "expected a finite number" })],
    }));
  });

  it("awaits asynchronous Standard Schema validation", async () => {
    const asyncSchema = {
      "~standard": {
        version: 1 as const,
        vendor: "durably-test",
        validate: async (value: unknown) => {
          await Promise.resolve();
          return typeof value === "string"
            ? { value: value.toUpperCase() }
            : { issues: [{ message: "expected string" }] };
        },
      },
    };
    const wf = workflow(async (ctx) =>
      ctx.step(() => Promise.resolve("valid"), { name: "async-schema", schema: asyncSchema })
    );
    expect((await testEngine().run(wf, undefined)).result).toBe("VALID");
  });

  it("retries a timed-out attempt with a fresh signal and can then succeed", async () => {
    const te = testEngine();
    let attempts = 0;
    const signals: AbortSignal[] = [];
    const wf = workflow(async (ctx) =>
      ctx.step(({ signal }) => {
        attempts++;
        signals.push(signal);
        if (attempts === 2) return Promise.resolve("recovered");
        return new Promise<never>((_resolve, reject) => {
          signal.addEventListener("abort", () => reject(signal.reason), { once: true });
        });
      }, {
        name: "timeout-then-retry",
        timeoutMs: 100,
        retry: { attempts: 2, backoff: "none" },
      })
    );
    const pending = te.run(wf, undefined);
    await tick();
    te.clock.advance(100);
    const result = await pending;
    expect(result.result).toBe("recovered");
    expect(signals).toHaveLength(2);
    expect(signals[0]?.aborted).toBe(true);
    expect(signals[1]?.aborted).toBe(false);
  });

  it("revalidates recorded outputs during replay without re-executing effects", async () => {
    let valid = true;
    let calls = 0;
    const mutableSchema = {
      "~standard": {
        version: 1 as const,
        vendor: "durably-test",
        validate: (value: unknown) =>
          valid && typeof value === "number"
            ? { value }
            : { issues: [{ message: "schema changed" }] },
      },
    };
    const wf = workflow(async (ctx) => {
      await ctx.step(() => Promise.resolve(++calls), {
        name: "validated",
        schema: mutableSchema,
      });
      return ctx.step(() => Promise.resolve("done"), { name: "after" });
    });
    const te = testEngine();
    const crashed = await te.run(wf, undefined, { crashAfter: "validated" });
    valid = false;

    const resumed = await te.resume(crashed.runId);
    expect(resumed.status).toBe("failed");
    expect(calls).toBe(1);
    expect(resumed.steps[0]?.error).toBeInstanceOf(ValidationError);
  });

  it("runs compensations in reverse completion order after terminal failure", async () => {
    const events: string[] = [];
    const wf = workflow(async (ctx) => {
      await ctx.step(async () => {
        events.push("hold");
        return { holdId: "h1" };
      }, {
        name: "hold",
        compensate: async ({ holdId }) => {
          events.push(`release:${holdId}`);
        },
      });
      await ctx.step(async () => {
        events.push("charge");
        return { chargeId: "c1" };
      }, {
        name: "charge",
        compensate: async ({ chargeId }) => {
          events.push(`refund:${chargeId}`);
        },
      });
      await ctx.step(() => Promise.reject(new Error("publish failed")), { name: "publish" });
    });

    const result = await testEngine().run(wf, undefined);
    expect(result.status).toBe("failed");
    expect(events).toEqual(["hold", "charge", "refund:c1", "release:h1"]);
  });

  it("does not compensate a step that never completed", async () => {
    let compensationCalls = 0;
    const wf = workflow(async (ctx) => {
      await ctx.step(() => Promise.reject(new Error("hold failed")), {
        name: "hold",
        compensate: async () => {
          compensationCalls++;
        },
      });
    });

    await testEngine().run(wf, undefined);
    expect(compensationCalls).toBe(0);
  });

  it("records compensation failure and resumes the remaining rollback durably", async () => {
    const events: string[] = [];
    let refundAttempts = 0;
    const wf = workflow(async (ctx) => {
      await ctx.step(() => Promise.resolve("hold"), {
        name: "hold",
        compensate: async () => {
          events.push("release");
        },
      });
      await ctx.step(() => Promise.resolve("charge"), {
        name: "charge",
        compensate: async () => {
          refundAttempts++;
          if (refundAttempts === 1) throw new Error("refund transport failed");
          events.push("refund");
        },
      });
      await ctx.step(() => Promise.reject(new Error("terminal")), { name: "terminal" });
    });
    const te = testEngine();
    const failedRollback = await te.run(wf, undefined);
    expect(failedRollback.status).toBe("failed");
    expect(failedRollback.compensationError).toEqual(expect.objectContaining({
      message: "refund transport failed",
    }));

    const resumed = await te.resume(failedRollback.runId);
    expect(resumed.status).toBe("failed");
    expect(events).toEqual(["refund", "release"]);
    expect(refundAttempts).toBe(2);
  });

  it("resumes a crash during rollback without re-running completed compensations", async () => {
    const compensationCalls = { first: 0, second: 0 };
    const wf = workflow(async (ctx) => {
      await ctx.step(() => Promise.resolve("first"), {
        name: "first",
        compensate: async () => {
          compensationCalls.first++;
        },
      });
      await ctx.step(() => Promise.resolve("second"), {
        name: "second",
        compensate: async () => {
          compensationCalls.second++;
        },
      });
      await ctx.step(() => Promise.reject(new Error("terminal")), { name: "terminal" });
    });
    const te = testEngine();

    const crashed = await te.run(wf, undefined, { crashAfter: "compensate:second" });
    expect(crashed.status).toBe("crashed");
    const done = await te.resume(crashed.runId);

    expect(done.status).toBe("failed");
    expect(compensationCalls).toEqual({ first: 1, second: 1 });
  });

  it("enforces concurrencyLimit for equal concurrency keys", async () => {
    const gates = [deferred<void>(), deferred<void>(), deferred<void>()];
    let active = 0;
    let peak = 0;
    const wf = workflow(async (ctx) =>
      ctx.parallel(gates.map((gate, index) => () =>
        ctx.step(async () => {
          active++;
          peak = Math.max(peak, active);
          await gate.promise;
          active--;
          return index;
        }, {
          name: `job-${index}`,
          concurrencyKey: "same-resource",
          concurrencyLimit: 2,
        })
      ))
    );
    const pending = testEngine().run(wf, undefined);
    await tick();

    expect(active).toBe(2);
    gates[0]!.resolve();
    await tick();
    expect(active).toBe(2);
    gates[1]!.resolve();
    gates[2]!.resolve();
    const done = await pending;

    expect(done.status).toBe("completed");
    expect(peak).toBe(2);
  });

  it("does not serialize different concurrency keys", async () => {
    const gates = [deferred<void>(), deferred<void>(), deferred<void>()];
    let active = 0;
    let peak = 0;
    const wf = workflow(async (ctx) =>
      ctx.parallel(gates.map((gate, index) => () =>
        ctx.step(async () => {
          active++;
          peak = Math.max(peak, active);
          await gate.promise;
          active--;
        }, {
          name: `job-${index}`,
          concurrencyKey: `resource-${index}`,
        })
      ))
    );
    const pending = testEngine().run(wf, undefined);
    await tick();

    expect(active).toBe(3);
    for (const gate of gates) gate.resolve();
    await pending;
    expect(peak).toBe(3);
  });

  it("throws BudgetExceededError at charge once a run budget is exceeded", async () => {
    const wf = workflow(async (ctx) => {
      ctx.charge({ usd: 0.6 });
      ctx.charge({ usd: 0.5 });
      return "unreachable";
    });

    const result = await testEngine().run(wf, undefined, { budget: { usd: 1 } });
    expect(result.status).toBe("failed");
    expect(result.error).toBeInstanceOf(BudgetExceededError);
    expect(result.error).toEqual(expect.objectContaining({ kind: "usd" }));
  });

  it("reports spent and remaining budget for each independent kind", async () => {
    const wf = workflow(async (ctx) => {
      ctx.charge({ usd: 0.25, tokens: 200 });
      return {
        usdSpent: ctx.budget.spent("usd"),
        usdRemaining: ctx.budget.remaining("usd"),
        tokensSpent: ctx.budget.spent("tokens"),
        tokenRemaining: ctx.budget.remaining("tokens"),
      };
    });

    const result = await testEngine().run(wf, undefined, {
      budget: { usd: 1, tokens: 1_000 },
    });
    expect(result.result).toEqual({
      usdSpent: 0.25,
      usdRemaining: 0.75,
      tokensSpent: 200,
      tokenRemaining: 800,
    });
  });
});
