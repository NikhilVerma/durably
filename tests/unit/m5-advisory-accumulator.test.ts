import { describe, expect, it } from "bun:test";
import { AdvisoryAccumulator } from "../../src/advisories.ts";

const occurrence = (code: string, level: "info" | "warn" = "info") => ({
  code,
  level,
  msg: `message for ${code}`,
  hint: `implemented move for ${code}`,
  docs: `durably.dev/test/${code.toLowerCase()}`,
  at: "2026-07-24T00:00:00.000Z",
});

describe("M5 internal advisory accumulator anti-spam contract", () => {
  it("enforces the ten-active cap with synthetic internal registry entries", () => {
    const emitted: unknown[] = [];
    const accumulator = new AdvisoryAccumulator({ maxActive: 10, emit: (item) => emitted.push(item) });
    for (let index = 0; index < 12; index++) {
      accumulator.observe(occurrence(`INTERNAL_TEST_${index}`));
    }
    expect(accumulator.active()).toHaveLength(10);
    expect(emitted).toHaveLength(10);
    expect(accumulator.active().map((item) => item.code)).toEqual(
      Array.from({ length: 10 }, (_, index) => `INTERNAL_TEST_${index}`),
    );
  });

  it("deduplicates by code, increments count, preserves first message, and emits crossings", () => {
    const emitted: Array<{ code: string; level: string }> = [];
    const accumulator = new AdvisoryAccumulator({ maxActive: 10, emit: (item) => emitted.push(item) });
    accumulator.observe(occurrence("LOOP_SUGGESTED", "info"));
    accumulator.observe({ ...occurrence("LOOP_SUGGESTED", "info"), msg: "must not replace" });
    accumulator.observe({ ...occurrence("LOOP_SUGGESTED", "warn"), msg: "must still not replace" });

    expect(accumulator.active()).toEqual([
      expect.objectContaining({
        code: "LOOP_SUGGESTED",
        level: "warn",
        count: 3,
        msg: "message for LOOP_SUGGESTED",
      }),
    ]);
    expect(emitted).toEqual([
      expect.objectContaining({ code: "LOOP_SUGGESTED", level: "info" }),
      expect.objectContaining({ code: "LOOP_SUGGESTED", level: "warn" }),
    ]);
  });

  it("scopes retry streaks by run, step path, and error class and resets on success", () => {
    const accumulator = new AdvisoryAccumulator({ maxActive: 10 });
    for (let attempt = 1; attempt <= 100; attempt++) {
      accumulator.retryFailed({
        runId: "run-a",
        path: [0],
        errorClass: "TypeError",
        attempt,
      });
    }
    expect(accumulator.active().some((item) => item.code === "RETRY_STORM")).toBe(true);

    accumulator.retrySucceeded({ runId: "run-a", path: [0] });
    const before = accumulator.active().find((item) => item.code === "RETRY_STORM")!.count;
    accumulator.retryFailed({ runId: "run-a", path: [0], errorClass: "TypeError", attempt: 1 });
    accumulator.retryFailed({ runId: "run-a", path: [1], errorClass: "TypeError", attempt: 2 });
    accumulator.retryFailed({ runId: "run-b", path: [0], errorClass: "TypeError", attempt: 3 });
    accumulator.retryFailed({ runId: "run-a", path: [0], errorClass: "RangeError", attempt: 4 });
    expect(accumulator.active().find((item) => item.code === "RETRY_STORM")!.count).toBe(before);
  });
});
