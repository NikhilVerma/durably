import { docsRoutes } from "../docs/routes.ts";

export interface RegistryEntry {
  readonly msg: string;
  readonly hint: string;
  readonly route: keyof typeof docsRoutes;
  readonly docs: string;
  readonly apiRefs: {
    readonly exports: readonly string[];
    readonly context: readonly string[];
    readonly engine: readonly string[];
  };
}

const entry = (
  route: keyof typeof docsRoutes,
  msg: string,
  hint: string,
  apiRefs: RegistryEntry["apiRefs"],
): RegistryEntry => ({ route, docs: docsRoutes[route], msg, hint, apiRefs });

export const advisoryRegistry = {
  LOOP_SUGGESTED: entry(
    "loop",
    "Many positional steps form a loop-shaped pattern; replay cost grows linearly.",
    "ctx.loop checkpoints state snapshots so resume becomes O(1).",
    { exports: [], context: ["loop"], engine: [] },
  ),
  FANOUT_CEILING: entry(
    "fanout",
    "Child-run fan-out is approaching the practical storage ceiling.",
    "Use a provider batch API or SqliteStorage for larger durable fan-out.",
    { exports: [], context: ["spawnAll"], engine: [] },
  ),
  RETRY_STORM: entry(
    "resources",
    "One step is repeatedly failing with the same error class.",
    "Attach a named resource breaker or reduce the retry policy.",
    { exports: ["resource"], context: ["step"], engine: [] },
  ),
  STASH_SUGGESTED: entry(
    "stash",
    "A long interrupted step restarted from zero without recovery progress.",
    "Use ctx.step stash and stashed to resume partial work.",
    { exports: [], context: ["step"], engine: [] },
  ),
  BUDGET_NEAR: entry(
    "budgets",
    "The run has consumed at least 80 percent of a configured budget.",
    "Read ctx.budget.remaining or raise the explicit run budget.",
    { exports: [], context: ["budget", "charge"], engine: [] },
  ),
  WAITFOR_NO_TIMEOUT: entry(
    "wait-for",
    "A waitFor call can park this run indefinitely.",
    "Pass timeoutMs to ctx.waitFor when an unbounded wait is not intended.",
    { exports: [], context: ["waitFor"], engine: [] },
  ),
  SNAPSHOT_HEAVY: entry(
    "loop#snapshot-size",
    "Loop snapshot size and frequency are producing heavy persistence traffic.",
    "Compact loop state or increase snapshotEvery.",
    { exports: [], context: ["loop"], engine: [] },
  ),
  SLOW_STEP_NO_TIMEOUT: entry(
    "timeouts",
    "A step attempt exceeded 30 seconds without a timeout policy.",
    "Set step timeoutMs and handle its AbortSignal.",
    { exports: [], context: ["step"], engine: [] },
  ),
} as const satisfies Record<string, RegistryEntry>;
