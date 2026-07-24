import { describe, expect, it } from "bun:test";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { findFilesNamed, withTempDir } from "../support/kit.ts";

const worker = fileURLToPath(new URL("../fixtures/readme-hero-worker.ts", import.meta.url));
const waitLine = (child: ChildProcessWithoutNullStreams, prefix: string, timeoutMs = 5_000) =>
  new Promise<string>((resolve, reject) => {
    let text = "";
    let errors = "";
    const timeout = setTimeout(() => reject(new Error(`timeout waiting for ${prefix}: ${text}`)), timeoutMs);
    child.stderr.on("data", (chunk) => {
      errors += String(chunk);
    });
    child.stdout.on("data", (chunk) => {
      text += String(chunk);
      const line = text.split("\n").find((value) => value.startsWith(prefix));
      if (line) {
        clearTimeout(timeout);
        resolve(line.slice(prefix.length).trim());
      }
    });
    child.once("exit", (code, signal) => {
      if (!text.includes(prefix)) {
        clearTimeout(timeout);
        reject(new Error(`hero worker exited before ${prefix}: code=${code} signal=${signal}\n${errors}`));
      }
    });
  });
const waitForDistinctCount = async (
  path: string,
  count: number,
  child?: ChildProcessWithoutNullStreams,
) => {
  const deadline = Date.now() + 5_000;
  for (;;) {
    if (child && (child.exitCode !== null || child.signalCode !== null)) {
      throw new Error(`hero worker exited before producing ${count} distinct values in ${path}`);
    }
    try {
      const found = (await readFile(path, "utf8")).trim().split("\n").filter(Boolean).map(Number);
      if (new Set(found).size >= count) return;
    } catch {}
    if (Date.now() >= deadline) {
      throw new Error(`timeout waiting for ${count} distinct values in ${path}`);
    }
    await new Promise((resolve) => setImmediate(resolve));
  }
};

describe("literal README hero in real processes", () => {
  it("kills mid fan-out, reruns the same command, and never rebills completed effects", async () => {
    await withTempDir(async (dir) => {
      const start = () => spawn(process.execPath, [worker, dir], {
        cwd: fileURLToPath(new URL("../..", import.meta.url)),
        stdio: ["ignore", "pipe", "pipe"],
      });
      const first = start();
      let second: ChildProcessWithoutNullStreams | undefined;
      try {
        for (let index = 0; index < 4; index++) {
          await writeFile(`${dir}/release-fetch-${index}`, "");
        }
        await waitForDistinctCount(`${dir}/entered-generate.log`, 2, first);
        const stateFiles = await findFilesNamed(dir, "state.json");
        expect(stateFiles.length).toBeGreaterThan(0);
        const states = await Promise.all(stateFiles.map(async (path) =>
          JSON.parse(await readFile(path, "utf8")) as { runId?: string; children?: string[] }
        ));
        const parentState = states.find((state) => (state.children?.length ?? 0) > 0)!;
        expect(parentState).toBeDefined();
        first.kill("SIGKILL");
        await new Promise((resolve) => first.once("exit", resolve));

        second = start();
        for (let index = 0; index < 4; index++) {
          await writeFile(`${dir}/release-fetch-${index}`, "");
          await writeFile(`${dir}/release-generate-${index}`, "");
        }
        const result = JSON.parse(await waitLine(second, "DONE ")) as {
          intro: string; items: unknown[]; failed: number;
        };
        expect(result).toEqual({
          intro: "intro:4",
          items: Array.from({ length: 4 }, (_, index) => ({ index, summary: `summary:page-${index}` })),
          failed: 0,
        });
        expect(parentState.runId).toEqual(expect.any(String));
        const fetches = (await readFile(`${dir}/fetch.log`, "utf8")).trim().split("\n").map(Number);
        expect(fetches.sort((a, b) => a - b)).toEqual([0, 1, 2, 3]);
        const generations = (await readFile(`${dir}/generate.log`, "utf8")).trim().split("\n").map(Number);
        expect(generations.sort((a, b) => a - b)).toEqual([0, 1, 1, 2, 3, 3]);
        expect((await readFile(`${dir}/intro.log`, "utf8")).trim().split("\n")).toEqual(["intro"]);
      } finally {
        if (first.exitCode === null && first.signalCode === null) first.kill("SIGKILL");
        if (second && second.exitCode === null && second.signalCode === null) second.kill("SIGKILL");
      }
    });
  }, 15_000);
});
