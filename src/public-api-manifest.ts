export const publicApiManifest = {
  contextMembers: [
    "step", "loop", "parallel", "spawn", "spawnAll", "joinAll", "sleep", "sleepUntil",
    "waitFor", "complete", "now", "random", "log", "budget", "charge", "annotate",
    "runId", "attempt",
  ],
  engineMembers: [
    "enqueue", "signal", "cancel", "pause", "resume", "inspect", "list", "retry",
    "restart", "adopt", "drain", "stop",
  ],
  docsPages: [
    "loop", "fanout", "resources", "stash", "budgets", "wait-for", "timeouts",
    "validation", "serialization", "purity", "staleness", "keys", "cancellation", "leases",
  ],
  docsAnchors: {
    loop: ["snapshot-size"],
  } as Record<string, string[]>,
} as const;
