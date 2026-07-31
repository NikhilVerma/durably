import { describe, expect, it } from "bun:test";
import { spawn } from "node:child_process";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { withTempDir } from "../support/kit.ts";

const worker = fileURLToPath(new URL("../fixtures/checkpoint-worker.ts", import.meta.url));

const runWorker = (
  dir: string,
  ledger: string,
  killAfter: number,
  checkpointEvery: number,
): Promise<{ code: number | null; signal: string | null; stderr: string }> =>
  new Promise((resolve, reject) => {
    const child = spawn(
      process.execPath,
      [worker, dir, ledger, String(killAfter), String(checkpointEvery)],
      { stdio: ["ignore", "pipe", "pipe"] },
    );
    let stderr = "";
    child.stderr.on("data", (chunk) => {
      stderr += String(chunk);
    });
    child.once("error", reject);
    child.once("exit", (code, signal) => resolve({ code, signal, stderr }));
  });

const executed = async (path: string): Promise<number[]> =>
  (await readFile(path, "utf8")).trim().split("\n").filter(Boolean).map(Number);

describe("checkpoint durability across a real process death", () => {
  it("costs at most checkpointEvery completed steps when a run is SIGKILLed", async () => {
    await withTempDir(async (dir) => {
      const durably = join(dir, ".durably");
      const first = join(dir, "first.txt");
      const second = join(dir, "second.txt");
      await writeFile(first, "");
      await writeFile(second, "");

      const killed = await runWorker(durably, first, 250, 5);
      expect(killed.signal).toBe("SIGKILL");
      const before = await executed(first);
      expect(before.at(-1)).toBe(250);

      const resumed = await runWorker(durably, second, -1, 5);
      expect(resumed.code).toBe(0);
      const after = await executed(second);

      const completedBefore = new Set(before.slice(0, -1));
      const repeated = after.filter((index) => completedBefore.has(index));
      // The killed process recorded step 250; everything before it was
      // checkpointed at most `checkpointEvery` steps ago.
      expect(repeated.length).toBeLessThanOrEqual(5);
      expect(after.at(-1)).toBe(299);
      expect(new Set([...before, ...after]).size).toBe(300);
    });
  }, 120_000);

  it("re-executes nothing already completed when checkpointEvery is 1", async () => {
    await withTempDir(async (dir) => {
      const durably = join(dir, ".durably");
      const first = join(dir, "first.txt");
      const second = join(dir, "second.txt");
      await writeFile(first, "");
      await writeFile(second, "");

      const killed = await runWorker(durably, first, 250, 1);
      expect(killed.signal).toBe("SIGKILL");
      const before = await executed(first);

      const resumed = await runWorker(durably, second, -1, 1);
      expect(resumed.code).toBe(0);
      const after = await executed(second);

      const completedBefore = new Set(before.slice(0, -1));
      expect(after.filter((index) => completedBefore.has(index))).toEqual([]);
      expect(after[0]).toBe(250);
    });
  }, 120_000);
});
