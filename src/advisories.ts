import { advisoryRegistry } from "./advisories/registry.ts";
import type { Advisory } from "../index.ts";

export interface AdvisoryOccurrence {
  readonly code: string;
  readonly level: "info" | "warn";
  readonly msg: string;
  readonly hint: string;
  readonly docs: string;
  readonly at: string;
}

export interface AdvisoryAccumulatorOptions {
  readonly maxActive?: number;
  readonly emit?: (advisory: Advisory) => void;
}

const severity = (level: "info" | "warn") => level === "warn" ? 1 : 0;

export class AdvisoryAccumulator {
  readonly #maxActive: number;
  readonly #emit: ((advisory: Advisory) => void) | undefined;
  readonly #active = new Map<string, Advisory>();
  readonly #emittedLevel = new Map<string, "info" | "warn">();
  readonly #retryStreaks = new Map<string, { errorClass: string; count: number }>();

  constructor(options: AdvisoryAccumulatorOptions = {}) {
    this.#maxActive = options.maxActive ?? 10;
    this.#emit = options.emit;
  }

  observe(occurrence: AdvisoryOccurrence): Advisory | undefined {
    const current = this.#active.get(occurrence.code);
    if (!current && this.#active.size >= this.#maxActive) return undefined;
    const next: Advisory = current
      ? {
          ...current,
          level: severity(occurrence.level) > severity(current.level)
            ? occurrence.level
            : current.level,
          count: current.count + 1,
        }
      : {
          code: occurrence.code as Advisory["code"],
          level: occurrence.level,
          msg: occurrence.msg,
          hint: occurrence.hint,
          docs: occurrence.docs,
          count: 1,
          firstAt: occurrence.at,
        };
    this.#active.set(occurrence.code, next);

    const emitted = this.#emittedLevel.get(occurrence.code);
    if (!emitted || severity(next.level) > severity(emitted)) {
      this.#emittedLevel.set(occurrence.code, next.level);
      this.#emit?.({ ...next });
    }
    return next;
  }

  active(): Advisory[] {
    return [...this.#active.values()].map((item) => ({ ...item }));
  }

  retryFailed(input: {
    readonly runId: string;
    readonly path: readonly number[];
    readonly errorClass: string;
    readonly attempt: number;
  }): void {
    const key = `${input.runId}:${input.path.join(".")}`;
    const previous = this.#retryStreaks.get(key);
    const next = previous?.errorClass === input.errorClass
      ? { errorClass: input.errorClass, count: previous.count + 1 }
      : { errorClass: input.errorClass, count: 1 };
    this.#retryStreaks.set(key, next);
    if (next.count === 10) {
      const definition = advisoryRegistry.RETRY_STORM;
      this.observe({
        code: "RETRY_STORM",
        level: "info",
        msg: definition.msg,
        hint: definition.hint,
        docs: definition.docs,
        at: new Date().toISOString(),
      });
    }
  }

  retrySucceeded(input: { readonly runId: string; readonly path: readonly number[] }): void {
    this.#retryStreaks.delete(`${input.runId}:${input.path.join(".")}`);
  }

  restore(advisories: readonly Advisory[]): void {
    for (const advisory of advisories) {
      this.#active.set(advisory.code, { ...advisory });
      this.#emittedLevel.set(advisory.code, advisory.level);
    }
  }
}
