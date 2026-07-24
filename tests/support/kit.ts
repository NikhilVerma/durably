import { mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

export const deferred = <T>() => {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
};

export const tick = () => new Promise<void>((resolve) => queueMicrotask(resolve));

export const withTempDir = async <T>(fn: (dir: string) => Promise<T>): Promise<T> => {
  const dir = await mkdtemp(join(tmpdir(), "durably-test-"));
  try {
    return await fn(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
};

export const readJson = async <T>(path: string): Promise<T> =>
  JSON.parse(await readFile(path, "utf8")) as T;

export const findFilesNamed = async (root: string, name: string): Promise<string[]> => {
  const matches: string[] = [];
  const visit = async (dir: string): Promise<void> => {
    for (const entry of await readdir(dir, { withFileTypes: true })) {
      const path = join(dir, entry.name);
      if (entry.isDirectory()) await visit(path);
      if (entry.isFile() && entry.name === name) matches.push(path);
    }
  };
  await visit(root);
  return matches.sort();
};

export const seededDelays = (count: number): number[] => {
  let state = 0x9e3779b9;
  return Array.from({ length: count }, () => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return Math.abs(state) % 7;
  });
};

export type StandardSchema<I, O> = {
  readonly "~standard": {
    readonly version: 1;
    readonly vendor: string;
    readonly validate: (
      value: unknown,
    ) =>
      | { readonly value: O }
      | { readonly issues: readonly { readonly message: string; readonly path?: readonly PropertyKey[] }[] }
      | Promise<
          | { readonly value: O }
          | { readonly issues: readonly { readonly message: string; readonly path?: readonly PropertyKey[] }[] }
        >;
    readonly types?: {
      readonly input: I;
      readonly output: O;
    };
  };
};

const schema = <T>(
  predicate: (value: unknown) => value is T,
  message = "invalid value",
): StandardSchema<unknown, T> => ({
  "~standard": {
    version: 1,
    vendor: "durably-tests",
    validate: (value) =>
      predicate(value)
        ? { value }
        : { issues: [{ message }] },
  },
});

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export const numberSchema = schema<number>(
  (value): value is number => typeof value === "number" && Number.isFinite(value),
  "expected a finite number",
);

export const objectWithStringIdSchema = schema<{ id: string }>(
  (value): value is { id: string } => isRecord(value) && typeof value.id === "string",
  "expected an object with a string id",
);
