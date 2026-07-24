import { describe, expect, it } from "bun:test";
import { advisoryRegistry } from "../../src/advisories/registry.ts";
import { errorRegistry } from "../../src/errors/registry.ts";
import { docsRoutes } from "../../src/docs/routes.ts";
import { publicApiManifest } from "../../src/public-api-manifest.ts";
import * as publicApi from "../../index.ts";

const contextMembers = new Set([
  "step", "loop", "parallel", "spawn", "spawnAll", "joinAll", "sleep", "sleepUntil",
  "waitFor", "complete", "now", "random", "log", "budget", "charge", "annotate",
  "runId", "attempt",
]);
const engineMembers = new Set([
  "enqueue", "signal", "cancel", "pause", "resume", "inspect", "list", "retry",
  "restart", "adopt", "drain", "stop",
]);

describe("M8 actual hint and documentation registries", () => {
  it("routes every actual advisory and error entry through the exact docs manifest", () => {
    const entries = [...Object.values(advisoryRegistry), ...Object.values(errorRegistry)];
    expect(entries.length).toBeGreaterThan(8);
    for (const entry of entries) {
      expect(entry.hint.trim().length).toBeGreaterThan(10);
      expect(entry.docs).toBe(docsRoutes[entry.route]);
      expect(entry.docs).toMatch(
        /^https:\/\/www\.npmjs\.com\/package\/@nikhilverma\/durably#/,
      );
    }
    expect(new Set(Object.keys(advisoryRegistry))).toEqual(new Set([
      "LOOP_SUGGESTED", "FANOUT_CEILING", "RETRY_STORM", "STASH_SUGGESTED",
      "BUDGET_NEAR", "WAITFOR_NO_TIMEOUT", "SNAPSHOT_HEAVY", "SLOW_STEP_NO_TIMEOUT",
    ]));
  });

  it("resolves every route and fragment to a declared documentation anchor", () => {
    for (const [route, url] of Object.entries(docsRoutes)) {
      expect(url).toMatch(
        /^https:\/\/www\.npmjs\.com\/package\/@nikhilverma\/durably#[a-z0-9-]+$/,
      );
      const [page, fragment] = route.split("#");
      expect(publicApiManifest.docsPages).toContain(page);
      if (fragment) expect(publicApiManifest.docsAnchors[page!]).toContain(fragment);
    }
  });

  it("requires structured API refs and resolves them against actual surfaces", () => {
    for (const entry of [...Object.values(advisoryRegistry), ...Object.values(errorRegistry)]) {
      expect(entry.apiRefs).toEqual(expect.objectContaining({
        exports: expect.any(Array),
        context: expect.any(Array),
        engine: expect.any(Array),
      }));
      for (const reference of entry.apiRefs.exports) {
        expect(Object.prototype.hasOwnProperty.call(publicApi, reference)).toBe(true);
      }
      for (const reference of entry.apiRefs.context) expect(contextMembers).toContain(reference);
      for (const reference of entry.apiRefs.engine) expect(engineMembers).toContain(reference);
    }
    expect(new Set(publicApiManifest.contextMembers)).toEqual(contextMembers);
    expect(new Set(publicApiManifest.engineMembers)).toEqual(engineMembers);
  });
});
