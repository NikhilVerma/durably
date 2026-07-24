import { describe, expect, it } from "bun:test";
import { computeBackoffMs } from "../../src/policies/backoff.ts";

describe("M3 internal retry backoff policy", () => {
  it("applies deterministic jitter within bounds and never exceeds maxMs", () => {
    const low = computeBackoffMs({
      attempt: 10,
      backoff: "exponential",
      baseMs: 100,
      maxMs: 1_000,
      jitter: true,
      random: () => 0,
    });
    const middle = computeBackoffMs({
      attempt: 10,
      backoff: "exponential",
      baseMs: 100,
      maxMs: 1_000,
      jitter: true,
      random: () => 0.5,
    });
    const high = computeBackoffMs({
      attempt: 10,
      backoff: "exponential",
      baseMs: 100,
      maxMs: 1_000,
      jitter: true,
      random: () => 0.999999,
    });
    expect(low).toBeGreaterThanOrEqual(0);
    expect(low).toBeLessThan(middle);
    expect(middle).toBeLessThan(high);
    expect(new Set([low, middle, high]).size).toBe(3);
    expect(high).toBeLessThanOrEqual(1_000);
  });

  it("caps linear and exponential schedules before applying jitter", () => {
    for (const backoff of ["linear", "exponential"] as const) {
      expect(computeBackoffMs({
        attempt: 1_000,
        backoff,
        baseMs: 250,
        maxMs: 2_000,
        jitter: false,
        random: () => {
          throw new Error("random must not be called when jitter is disabled");
        },
      })).toBe(2_000);
    }
  });
});
