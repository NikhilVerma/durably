import { docsRoutes } from "../docs/routes.ts";
import type { RegistryEntry } from "../advisories/registry.ts";

const entry = (
  route: keyof typeof docsRoutes,
  hint: string,
  apiRefs: RegistryEntry["apiRefs"],
): RegistryEntry => ({
  route,
  docs: docsRoutes[route],
  msg: hint,
  hint,
  apiRefs,
});

export const errorRegistry = {
  ValidationError: entry(
    "validation",
    "Inspect validation issues and correct the value inside ctx.step",
    { exports: ["ValidationError"], context: ["step"], engine: [] },
  ),
  SerializationError: entry(
    "serialization",
    "Return plain JSON data or Date values from the producing step.",
    { exports: ["SerializationError"], context: ["step"], engine: [] },
  ),
  StepTimeoutError: entry(
    "timeouts",
    "Handle the step AbortSignal or increase timeoutMs.",
    { exports: ["StepTimeoutError"], context: ["step"], engine: [] },
  ),
  PurityError: entry(
    "purity",
    "Keep effects inside ctx.step and keep between-step control flow pure",
    { exports: ["PurityError"], context: ["step"], engine: [] },
  ),
  StaleRunError: entry(
    "staleness",
    "durably.restart(id) starts fresh; durably.adopt(id) resumes if you assert replay compatibility",
    { exports: ["StaleRunError", "durably"], context: [], engine: ["restart", "adopt"] },
  ),
  BudgetExceededError: entry(
    "budgets",
    "Inspect ctx.budget or start fresh with a larger explicit budget.",
    { exports: ["BudgetExceededError"], context: ["budget"], engine: [] },
  ),
  CircuitOpenError: entry(
    "resources",
    "Wait for the resource breaker cooldown or inspect the provider failure",
    { exports: ["CircuitOpenError", "resource"], context: [], engine: [] },
  ),
  KeyConflictError: entry(
    "keys",
    "Reuse the bound input, pass fresh, or inspect the run named in this error",
    { exports: ["KeyConflictError"], context: [], engine: ["inspect"] },
  ),
  RunCancelledError: entry(
    "cancellation",
    "Inspect the cancelled run and enqueue fresh work if needed",
    { exports: ["RunCancelledError"], context: [], engine: ["inspect", "enqueue"] },
  ),
  LeaseLostError: entry(
    "leases",
    "Use inspect to check whether another process reclaimed the run",
    { exports: ["LeaseLostError"], context: [], engine: ["inspect"] },
  ),
} as const satisfies Record<string, RegistryEntry>;
