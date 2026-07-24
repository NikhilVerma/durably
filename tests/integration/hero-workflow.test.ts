import { describe, expect, it } from "bun:test";
import { testEngine, workflow } from "../../index.ts";

describe("README hero workflow", () => {
  it("fans out child runs, tolerates flaky retries, joins values, and resumes only unfinished work", async () => {
    const fetchCalls = new Map<string, number>();
    const summarizeCalls = new Map<string, number>();
    let introCalls = 0;
    const flaky = new Set(["https://b.example", "https://d.example"]);

    const summarize = workflow<{ url: string }>(async (ctx, { url }) => {
      const page = await ctx.step(() => {
        fetchCalls.set(url, (fetchCalls.get(url) ?? 0) + 1);
        return Promise.resolve(`page:${url}`);
      }, { name: "fetch" });
      const summary = await ctx.step(() => {
        const attempt = (summarizeCalls.get(url) ?? 0) + 1;
        summarizeCalls.set(url, attempt);
        if (flaky.has(url) && attempt === 1) throw new Error("transient LLM error");
        return Promise.resolve({ text: `summary:${page}` });
      }, {
        name: "generateText",
        retry: { attempts: 4, backoff: "none" },
      });
      return { url, title: summary.text.slice(0, 24), summary: summary.text };
    });

    const digest = workflow<{ urls: string[] }>(async (ctx, { urls }) => {
      const kids = await ctx.spawnAll(urls.map((url) => [summarize, { url }] as const));
      const results = await ctx.joinAll(kids);
      const ok = results.filter((result) => result.ok).map((result) => result.value);
      const intro = await ctx.step(() => {
        introCalls++;
        return Promise.resolve(`intro:${ok.length}`);
      }, { name: "intro generateText" });
      return { intro, items: ok, failed: results.length - ok.length };
    });

    const urls = ["https://a.example", "https://b.example", "https://c.example", "https://d.example"];
    const te = testEngine();
    const crashed = await te.run(digest, { urls }, { crashAfter: "intro generateText" });
    expect(crashed.status).toBe("crashed");
    const done = await te.resume(crashed.runId);

    expect(done.status).toBe("completed");
    expect(done.result).toEqual(expect.objectContaining({
      intro: "intro:4",
      failed: 0,
      items: expect.arrayContaining(urls.map((url) => expect.objectContaining({ url }))),
    }));
    expect([...fetchCalls.values()]).toEqual([1, 1, 1, 1]);
    expect(Object.fromEntries(summarizeCalls)).toEqual({
      "https://a.example": 1,
      "https://b.example": 2,
      "https://c.example": 1,
      "https://d.example": 2,
    });
    expect(introCalls).toBe(1);
  });

  it("keeps permanently failed children as values and still produces the digest", async () => {
    const summarize = workflow<{ url: string }>(async (ctx, { url }) =>
      ctx.step(() => {
        if (url.includes("bad")) throw new Error(`cannot fetch ${url}`);
        return Promise.resolve({ url, title: url, summary: `summary:${url}` });
      }, { name: "summarize" })
    );
    const digest = workflow<{ urls: string[] }>(async (ctx, { urls }) => {
      const kids = await ctx.spawnAll(urls.map((url) => [summarize, { url }] as const));
      const outcomes = await ctx.joinAll(kids);
      const ok = outcomes.filter((outcome) => outcome.ok).map((outcome) => outcome.value);
      return {
        items: ok,
        failed: outcomes.length - ok.length,
        errors: outcomes.filter((outcome) => !outcome.ok).map((outcome) => outcome.error.message),
      };
    });

    const result = await testEngine().run(digest, {
      urls: ["good-1", "bad-1", "good-2", "bad-2"],
    });

    expect(result.status).toBe("completed");
    expect(result.result.items.map((item) => item.url)).toEqual(["good-1", "good-2"]);
    expect(result.result.failed).toBe(2);
    expect(result.result.errors).toEqual(["cannot fetch bad-1", "cannot fetch bad-2"]);
  });
});

describe("whole-workflow durability properties", () => {
  it("produces the same result when killed at every possible boundary", async () => {
    const build = (counts: number[]) =>
      workflow<{ values: number[] }>(async (ctx, input) => {
        const output: number[] = [];
        for (const [index, value] of input.values.entries()) {
          output.push(await ctx.step(() => {
            counts[index] = (counts[index] ?? 0) + 1;
            return Promise.resolve(value * 2);
          }, { name: `step-${index}` }));
        }
        return output.reduce((sum, value) => sum + value, 0);
      });
    const input = { values: [3, 1, 4, 1, 5, 9, 2, 6] };
    const expected = input.values.reduce((sum, value) => sum + value * 2, 0);

    for (let boundary = 0; boundary < input.values.length; boundary++) {
      const counts: number[] = [];
      const te = testEngine();
      const crashed = await te.run(build(counts), input, {
        crashAfter: `step-${boundary}`,
      });
      const resumed = await te.resume(crashed.runId);

      expect(resumed.result).toBe(expected);
      expect(counts).toEqual(input.values.map(() => 1));
      expect(resumed.steps.every((step) => step.executions === 1)).toBe(true);
    }
  });

  it("combines saga rollback, a rollback crash, and durable resume", async () => {
    const inventory = new Set<string>();
    const charges = new Set<string>();
    const events: string[] = [];
    const checkout = workflow<{ orderId: string }>(async (ctx, input) => {
      await ctx.step(() => {
        inventory.add(input.orderId);
        events.push("hold");
        return Promise.resolve(input.orderId);
      }, {
        name: "hold",
        compensate: async (orderId) => {
          inventory.delete(orderId);
          events.push("release");
        },
      });
      await ctx.step(() => {
        charges.add(input.orderId);
        events.push("charge");
        return Promise.resolve(input.orderId);
      }, {
        name: "charge",
        compensate: async (orderId) => {
          charges.delete(orderId);
          events.push("refund");
        },
      });
      await ctx.step(() => Promise.reject(new Error("shipping unavailable")), {
        name: "ship",
        retry: { attempts: 3, backoff: "none" },
      });
    });
    const te = testEngine();

    const crashed = await te.run(checkout, { orderId: "o-1" }, {
      crashAfter: "compensate:charge",
    });
    expect(crashed.status).toBe("crashed");
    const done = await te.resume(crashed.runId);

    expect(done.status).toBe("failed");
    expect(inventory.size).toBe(0);
    expect(charges.size).toBe(0);
    expect(events).toEqual(["hold", "charge", "refund", "release"]);
  });
});
