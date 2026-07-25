import { describe, expect, it } from "bun:test";
import { appendFile, readFile, stat, truncate, writeFile } from "node:fs/promises";
import {
  FileStorage,
  MemoryStorage,
  StepTimeoutError,
  createEngine,
  run,
  workflow,
  type RunClaim,
  type StorageAdapter,
  type StorageListOptions,
  type StoredRun,
} from "../../index.ts";
import { findFilesNamed, readJson, withTempDir } from "../support/kit.ts";

const crc32 = (bytes: Uint8Array): number => {
  let crc = 0xffffffff;
  for (const byte of bytes) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ (0xedb88320 & -(crc & 1));
  }
  return (crc ^ 0xffffffff) >>> 0;
};

type Frame = { start: number; end: number; payload: Buffer; event: Record<string, unknown> };

const parseFrames = (bytes: Buffer): Frame[] => {
  const parse = (endian: "LE" | "BE"): Frame[] | null => {
    const frames: Frame[] = [];
    let offset = 0;
    while (offset < bytes.length) {
      if (offset + 8 > bytes.length) return null;
      const length = endian === "LE" ? bytes.readUInt32LE(offset) : bytes.readUInt32BE(offset);
      const expected = endian === "LE" ? bytes.readUInt32LE(offset + 4) : bytes.readUInt32BE(offset + 4);
      const end = offset + 8 + length;
      if (end > bytes.length) return null;
      const payload = bytes.subarray(offset + 8, end);
      if (crc32(payload) !== expected) return null;
      try {
        frames.push({
          start: offset,
          end,
          payload,
          event: JSON.parse(payload.toString("utf8").trim()) as Record<string, unknown>,
        });
      } catch {
        return null;
      }
      offset = end;
    }
    return frames;
  };
  const frames = parse("LE") ?? parse("BE");
  if (!frames) throw new Error("events.log contains an invalid frame, CRC, or NDJSON payload");
  return frames;
};

describe("M4 storage adapter contract", () => {
  it("memory and file storage expose the seven required event-log operations", async () => {
    await withTempDir(async (dir) => {
      for (const storage of [new MemoryStorage(), new FileStorage(dir)] satisfies StorageAdapter[]) {
        for (const method of ["init", "createRun", "append", "read", "claim", "heartbeat", "list"]) {
          expect(storage[method as keyof StorageAdapter]).toBeFunction();
        }
      }
    });
  });

  it("materializes inspectable state.json and events.log for a file-backed run", async () => {
    await withTempDir(async (dir) => {
      const wf = workflow<{ id: string }>()(async (ctx, input) => {
        await ctx.step(() => Promise.resolve({ fetched: input.id }), { name: "fetch" });
        return ctx.waitFor("approval");
      });

      const engine = createEngine({ storage: new FileStorage(dir) });
      await engine.start();
      const handle = await engine.enqueue(wf, { id: "doc-1" });
      await engine.drain();

      const stateFiles = await findFilesNamed(dir, "state.json");
      const eventFiles = await findFilesNamed(dir, "events.log");
      expect(stateFiles).toHaveLength(1);
      expect(eventFiles).toHaveLength(1);
      expect((await stat(eventFiles[0]!)).size).toBeGreaterThan(0);

      const diskState = await readJson<Record<string, unknown>>(stateFiles[0]!);
      expect(diskState).toEqual(expect.objectContaining({
        status: "waiting",
        workflow: expect.any(String),
        waitingFor: expect.objectContaining({ signal: "approval" }),
        steps: [
          expect.objectContaining({
            path: [0],
            label: "fetch",
            status: "ok",
          }),
        ],
      }));
      expect(await engine.inspect(handle.runId)).toEqual(diskState);
      await engine.stop();
    });
  });

  it("writes state.json atomically without leaving temporary siblings", async () => {
    await withTempDir(async (dir) => {
      const wf = workflow(async (ctx) => {
        for (let index = 0; index < 20; index++) {
          await ctx.step(() => Promise.resolve(index), { name: `step-${index}` });
          ctx.annotate({ index });
        }
        return "done";
      });

      await run(wf, undefined, { dir });

      const temporaryStateFiles = (await findFilesNamed(dir, "state.json.tmp"))
        .concat(await findFilesNamed(dir, "state.tmp"));
      expect(temporaryStateFiles).toEqual([]);
      const [statePath] = await findFilesNamed(dir, "state.json");
      expect(await readJson(statePath!)).toEqual(expect.objectContaining({
        status: "completed",
        annotations: { index: 19 },
      }));
    });
  });

  it("keeps events append-only while state.json reflects current state", async () => {
    await withTempDir(async (dir) => {
      const wf = workflow(async (ctx) => {
        await ctx.step(() => Promise.resolve(1), { name: "one" });
        await ctx.step(() => Promise.resolve(2), { name: "two" });
        return "done";
      });
      await run(wf, undefined, { dir });

      const [eventsPath] = await findFilesNamed(dir, "events.log");
      const bytes = await readFile(eventsPath!);
      const frames = parseFrames(bytes);
      expect(frames.length).toBeGreaterThanOrEqual(3);
      expect(frames.at(-1)!.end).toBe(bytes.length);
      const sequences = frames.map((frame) => frame.event.seq).filter((seq) => typeof seq === "number");
      if (sequences.length > 0) {
        expect(sequences).toEqual([...sequences].sort((a, b) => (a as number) - (b as number)));
        expect(new Set(sequences).size).toBe(sequences.length);
      }
    });
  });

  it("self-heals an appended torn record tail without losing valid events", async () => {
    await withTempDir(async (dir) => {
      let firstCalls = 0;
      const wf = workflow(async (ctx) => {
        await ctx.step(() => Promise.resolve(++firstCalls), { name: "first" });
        return ctx.waitFor("approval");
      });
      const engine = createEngine({ storage: new FileStorage(dir) });
      await engine.start();
      const handle = await engine.enqueue(wf, undefined);
      await engine.drain();
      const [eventsPath] = await findFilesNamed(dir, "events.log");
      const validSize = (await stat(eventsPath!)).size;

      await appendFile(eventsPath!, Buffer.from([0xff, 0xff, 0xff]));
      await engine.stop();

      const recovered = createEngine({ storage: new FileStorage(dir) });
      await recovered.start();
      expect(await recovered.inspect(handle.runId)).toEqual(expect.objectContaining({
        status: "waiting",
      }));
      expect((await stat(eventsPath!)).size).toBe(validSize);
      expect(firstCalls).toBe(1);
      await recovered.stop();
    });
  });

  it("truncates every partial byte offset of the final record to the last valid boundary", async () => {
    await withTempDir(async (dir) => {
      const wf = workflow(async (ctx) => {
        await ctx.step(() => Promise.resolve("stable"), { name: "stable" });
        return ctx.waitFor("approval");
      });
      const engine = createEngine({ storage: new FileStorage(dir) });
      await engine.start();
      const handle = await engine.enqueue(wf, undefined);
      await engine.drain();
      await engine.stop();

      const [eventsPath] = await findFilesNamed(dir, "events.log");
      const completeLog = await readFile(eventsPath!);
      const frames = parseFrames(completeLog);
      const finalStart = frames.at(-1)!.start;
      const finalRecordBytes = frames.at(-1)!.end - finalStart;
      expect(finalStart).toBeGreaterThan(0);

      for (let retained = 0; retained < finalRecordBytes; retained++) {
        await writeFile(eventsPath!, completeLog);
        await truncate(eventsPath!, finalStart + retained);
        const storage = new FileStorage(dir);
        await storage.init();
        expect(await storage.read(handle.runId)).toEqual(frames.slice(0, -1).map((frame) => frame.event));
        expect((await stat(eventsPath!)).size).toBe(finalStart);
      }
    });
  });

  it.each(["length", "crc", "payload"] as const)(
    "truncates at the exact frame boundary for corrupted %s bytes",
    async (field) => {
      await withTempDir(async (dir) => {
        const engine = createEngine({ storage: new FileStorage(dir) });
        const wf = workflow(async (ctx) => {
          await ctx.step(() => Promise.resolve("one"), { name: "one" });
          await ctx.step(() => Promise.resolve("two"), { name: "two" });
          return ctx.waitFor("approval");
        });
        await engine.start();
        const handle = await engine.enqueue(wf, undefined);
        await engine.drain();
        await engine.stop();
        const [eventsPath] = await findFilesNamed(dir, "events.log");
        const original = await readFile(eventsPath!);
        const frames = parseFrames(original);
        const target = frames.at(-1)!;
        const corrupted = Buffer.from(original);
        const offset = field === "length"
          ? target.start
          : field === "crc"
            ? target.start + 4
            : target.start + 8 + Math.floor(target.payload.length / 2);
        corrupted[offset] = corrupted[offset]! ^ 0xff;
        await writeFile(eventsPath!, corrupted);

        const storage = new FileStorage(dir);
        await storage.init();
        const recovered = await storage.read(handle.runId);
        const expected = frames.slice(0, -1).map((frame) => frame.event);
        expect(recovered).toEqual(expected);
        expect((await stat(eventsPath!)).size).toBe(target.start);
        expect(parseFrames(await readFile(eventsPath!)).map((frame) => frame.event)).toEqual(expected);
      });
    },
  );

  it("uses a run-id prefix shard while retaining the full id as the leaf", async () => {
    await withTempDir(async (dir) => {
      const engine = createEngine({ storage: new FileStorage(dir) });
      await engine.start();
      const handle = await engine.enqueue(workflow(async () => "done"), undefined);
      await engine.drain();
      const [statePath] = await findFilesNamed(dir, "state.json");
      const parts = statePath!.split("/");
      const idIndex = parts.indexOf(handle.runId);
      expect(idIndex).toBeGreaterThan(0);
      expect(parts[idIndex - 1]).toBe(handle.runId.slice(0, 2));
      await engine.stop();
    });
  });
});

describe("M4 hydration fidelity", () => {
  it("keeps Date values in recorded outputs, annotations, and results across a new engine", async () => {
    await withTempDir(async (dir) => {
      const recordedAt = new Date("2026-01-02T03:04:05.000Z");
      const startedAt = new Date("2026-05-05T00:00:00.000Z");
      let executions = 0;
      const wf = workflow({ name: "hydrated-dates" }, async (ctx) => {
        const produced = await ctx.step(() => {
          executions++;
          return Promise.resolve({ at: recordedAt, count: 1 });
        }, { name: "produce" });
        ctx.annotate({ startedAt });
        await ctx.waitFor("go");
        return { at: produced.at, isDate: produced.at instanceof Date, count: produced.count };
      });

      const first = createEngine({ storage: new FileStorage(dir) });
      await first.start();
      const handle = await first.enqueue(wf, undefined, { key: "resume-me" });
      await first.drain();
      await first.stop();

      const second = createEngine({ storage: new FileStorage(dir) });
      await second.start();
      const hydrated = await second.inspect(handle.runId) as unknown as {
        steps: { output?: { at?: unknown } }[];
      };
      expect(hydrated.steps[0]?.output?.at).toBeInstanceOf(Date);
      expect(hydrated.steps[0]?.output).toEqual({ at: recordedAt, count: 1 });
      expect((await second.inspect(handle.runId))?.annotations?.startedAt).toEqual(startedAt);

      await second.signal(handle.runId, "go", null);
      await second.drain();
      const final = await second.inspect(handle.runId);
      expect(final?.status).toBe("completed");
      expect(final?.result).toEqual({ at: recordedAt, isDate: true, count: 1 });
      expect(executions).toBe(1);
      await second.stop();
    });
  });

  it("keeps recorded errors as Error instances with their class and message", async () => {
    await withTempDir(async (dir) => {
      const wf = workflow({ name: "hydrated-errors" }, async (ctx) => {
        await ctx.step(() => {
          const error = new Error("card declined");
          error.name = "PaymentError";
          return Promise.reject(error);
        }, { name: "charge" });
        return "unreachable";
      });

      const first = createEngine({ storage: new FileStorage(dir) });
      await first.start();
      const handle = await first.enqueue(wf, undefined);
      await first.drain();
      await first.stop();

      const second = createEngine({ storage: new FileStorage(dir) });
      await second.start();
      const state = await second.inspect(handle.runId) as unknown as {
        status: string;
        error?: unknown;
        steps: { error?: unknown }[];
      };
      expect(state.status).toBe("failed");
      expect(state.error).toBeInstanceOf(Error);
      expect(state.error).toEqual(expect.objectContaining({
        name: "PaymentError",
        message: "card declined",
      }));
      expect(state.steps[0]?.error).toBeInstanceOf(Error);
      await second.stop();
    });
  });

  it("rebuilds durably error subclasses by class after hydration", async () => {
    await withTempDir(async (dir) => {
      const wf = workflow({ name: "hydrated-typed-errors" }, async (ctx) => {
        await ctx.step(
          ({ signal }) =>
            new Promise<never>((_resolve, reject) => {
              signal.addEventListener("abort", () => reject(signal.reason), { once: true });
            }),
          { name: "hangs", timeoutMs: 10 },
        );
        return "unreachable";
      });

      const first = createEngine({ storage: new FileStorage(dir) });
      await first.start();
      const handle = await first.enqueue(wf, undefined);
      await first.drain();
      await first.stop();

      const second = createEngine({ storage: new FileStorage(dir) });
      await second.start();
      const state = await second.inspect(handle.runId);
      expect(state?.error).toBeInstanceOf(StepTimeoutError);
      expect(state?.error).toEqual(expect.objectContaining({ timeoutMs: 10 }));
      await second.stop();
    });
  });
});

describe("M4 third-party storage adapter conformance", () => {
  class MinimalStorage implements StorageAdapter {
    readonly events = new Map<string, unknown[]>();
    readonly states = new Map<string, StoredRun>();
    readonly claims = new Map<string, RunClaim>();

    async init(): Promise<void> {}
    async createRun(storedRun: StoredRun): Promise<void> {
      this.states.set(storedRun.runId, storedRun);
      this.events.set(storedRun.runId, []);
    }
    async append(runId: string, event: unknown): Promise<void> {
      this.events.get(runId)?.push(event);
      const state = (event as { state?: StoredRun["state"] }).state;
      if (state) this.states.set(runId, { runId, state });
    }
    async read(runId: string): Promise<unknown[] | null> {
      return this.events.get(runId) ?? null;
    }
    async claim(runId: string, claim: RunClaim): Promise<boolean> {
      const existing = this.claims.get(runId);
      if (existing && existing.owner !== claim.owner && Date.parse(existing.expiresAt) > Date.now()) {
        return false;
      }
      this.claims.set(runId, claim);
      return true;
    }
    async heartbeat(runId: string, claim: RunClaim): Promise<boolean> {
      const existing = this.claims.get(runId);
      if (existing && existing.owner !== claim.owner) return false;
      this.claims.set(runId, claim);
      return true;
    }
    async list(options: StorageListOptions = {}): Promise<StoredRun[]> {
      return [...this.states.values()]
        .filter(({ state }) =>
          (!options.status || state.status === options.status)
          && (!options.workflow || state.workflow === options.workflow)
        )
        .slice(0, options.limit);
    }
  }

  it("drives a full run through an adapter implementing only the seven documented methods", async () => {
    const storage = new MinimalStorage();
    const engine = createEngine({ storage, concurrency: 2 });
    const wf = workflow<{ value: number }>()(async (ctx, input) => {
      const doubled = await ctx.step(() => Promise.resolve(input.value * 2), { name: "double" });
      await ctx.waitFor("approve");
      return doubled + 1;
    });
    await engine.start();

    const handle = await engine.enqueue(wf, { value: 20 });
    await engine.drain();
    expect((await engine.inspect(handle.runId))?.status).toBe("waiting");

    await engine.signal(handle.runId, "approve", { by: "operator" });
    await engine.drain();
    expect(await engine.inspect(handle.runId)).toEqual(expect.objectContaining({
      status: "completed",
      result: 41,
    }));
    expect(await engine.list({ status: "completed" })).toHaveLength(1);
    expect((storage.events.get(handle.runId) ?? []).length).toBeGreaterThan(0);
    await engine.stop();
  });

  it("keeps runs isolated between adapters without optional release or resource hooks", async () => {
    const storage = new MinimalStorage();
    const engine = createEngine({ storage });
    const wf = workflow(async (ctx) =>
      ctx.step(() => Promise.reject(new Error("provider down")), { name: "call" })
    );
    await engine.start();
    const handle = await engine.enqueue(wf, undefined);
    await engine.drain();

    expect((await engine.inspect(handle.runId))?.status).toBe("failed");
    expect(storage.claims.has(handle.runId)).toBe(true);
    await engine.stop();
  });
});
