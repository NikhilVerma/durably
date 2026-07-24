export interface BackoffInput {
  readonly attempt: number;
  readonly backoff: "exponential" | "linear" | "none";
  readonly baseMs?: number | undefined;
  readonly maxMs?: number | undefined;
  readonly jitter?: boolean | undefined;
  readonly random?: (() => number) | undefined;
}

export const computeBackoffMs = ({
  attempt,
  backoff,
  baseMs = 100,
  maxMs = Number.POSITIVE_INFINITY,
  jitter = false,
  random = Math.random,
}: BackoffInput): number => {
  const multiplier = backoff === "none"
    ? 0
    : backoff === "linear"
      ? Math.max(1, attempt)
      : 2 ** Math.max(0, attempt - 1);
  const capped = Math.min(maxMs, baseMs * multiplier);
  if (!jitter || capped === 0) return capped;
  return Math.min(maxMs, Math.floor(capped * Math.max(0, Math.min(1, random()))));
};
