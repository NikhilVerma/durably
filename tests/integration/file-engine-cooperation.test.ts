import { describe, expect, it } from "bun:test";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { FileStorage, createEngine, workflow } from "../../index.ts";
import { deferred, findFilesNamed, tick, withTempDir } from "../support/kit.ts";

const workerPath = fileURLToPath(new URL("../fixtures/lease-worker.ts", import.meta.url));

const waitForLine = (
  child: ChildProcessWithoutNullStreams,
  prefix: string,
  timeoutMs = 2_000,
): Promise<string> =>
  new Promise((resolve, reject) => {
    let buffered = "";
    const timeout = setTimeout(() => reject(new Error(`timed out waiting for ${prefix}`)), timeoutMs);
    const onData = (chunk: Buffer) => {
      buffered += chunk.toString("utf8");
      const line = buffered.split("\n").find((candidate) => candidate.startsWith(prefix));
      if (!line) return;
      clearTimeout(timeout);
      child.stdout.off("data", onData);
      resolve(line.slice(prefix.length).trim());
    };
    child.stdout.on("data", onData);
    child.once("exit", (code, signal) => {
      if (!buffered.includes(prefix)) {
        clearTimeout(timeout);
        reject(new Error(`worker exited before ${prefix}: code=${code} signal=${signal}`));
      }
    });
  });

const waitForExit = (child: ChildProcessWithoutNullStreams): Promise<void> =>
  child.exitCode !== null || child.signalCode !== null
    ? Promise.resolve()
    : new Promise((resolve) => child.once("exit", () => resolve()));

describe("file-backed engine cooperation", () => {
  it("persists advisories identically in state.json and inspect()", async () => {
    await withTempDir(async (dir) => {
      const engine = createEngine({ storage: new FileStorage(dir) });
      const wf = workflow(async (ctx) => ctx.waitFor("approval"));
      await engine.start();
      const handle = await engine.enqueue(wf, undefined);
      await engine.drain();

      const inspected = await engine.inspect(handle.runId);
      expect(inspected?.advisories).toEqual([
        expect.objectContaining({
          code: "WAITFOR_NO_TIMEOUT",
          level: "info",
          count: 1,
        }),
      ]);
      const statePaths = await findFilesNamed(dir, "state.json");
      const disk = JSON.parse(await readFile(statePaths[0]!, "utf8"));
      expect(disk.advisories).toEqual(inspected?.advisories);
      await engine.stop();
    });
  });

  it("two engines on one directory subscribe instead of executing the same keyed run twice", async () => {
    await withTempDir(async (dir) => {
      const gate = deferred<void>();
      let externalCharges = 0;
      const wf = workflow<{ account: string }>()(async (ctx, input) =>
        ctx.step(async () => {
          externalCharges++;
          await gate.promise;
          return { account: input.account, charged: true };
        }, { name: "charge-card" })
      );
      const first = createEngine({ storage: new FileStorage(dir) });
      const second = createEngine({ storage: new FileStorage(dir) });
      await first.start();
      await second.start();

      const owner = await first.enqueue(wf, { account: "a-1" }, { key: "payment-1" });
      await tick();
      const subscriber = await second.enqueue(wf, { account: "a-1" }, { key: "payment-1" });
      await tick();
      expect(subscriber.runId).toBe(owner.runId);
      expect(externalCharges).toBe(1);

      gate.resolve();
      await Promise.all([first.drain(), second.drain()]);
      expect(await first.inspect(owner.runId)).toEqual(expect.objectContaining({
        status: "completed",
        result: { account: "a-1", charged: true },
      }));
      expect(await second.inspect(owner.runId)).toEqual(await first.inspect(owner.runId));
      expect(externalCharges).toBe(1);
      await first.stop();
      await second.stop();
    });
  });

  it("a second process subscribes, then reclaims after the lease owner is killed", async () => {
    await withTempDir(async (dir) => {
      const startWorker = (mode: "owner" | "subscriber") =>
        spawn(process.execPath, [workerPath, mode, dir], {
          cwd: fileURLToPath(new URL("../..", import.meta.url)),
          stdio: ["ignore", "pipe", "pipe"],
        });
      const owner = startWorker("owner");
      let subscriber: ChildProcessWithoutNullStreams | undefined;
      try {
        const ownerInfo = JSON.parse(await waitForLine(owner, "OWNER ")) as { runId: string; pid: number };
        while (true) {
          try {
            const attempts = (await readFile(`${dir}/second-attempts.txt`, "utf8")).trim().split("\n");
            if (attempts.length === 1) break;
          } catch {
            // The owner has not entered its second attempt yet.
          }
          await new Promise((resolve) => setImmediate(resolve));
        }
        subscriber = startWorker("subscriber");
        const subscriberInfo = JSON.parse(await waitForLine(subscriber, "SUBSCRIBER ")) as { runId: string; pid: number };
        expect(subscriberInfo.runId).toBe(ownerInfo.runId);
        expect(subscriberInfo.pid).not.toBe(ownerInfo.pid);
        await waitForLine(subscriber, "HELD ");
        expect((await readFile(`${dir}/second-attempts.txt`, "utf8")).trim().split("\n")).toEqual([
          String(ownerInfo.pid),
        ]);

        owner.kill("SIGKILL");
        await waitForExit(owner);
        await writeFile(`${dir}/release`, "ok");
        const finalState = JSON.parse(await waitForLine(subscriber, "DONE ", 5_000)) as {
          status: string;
          result: unknown;
        };
        await waitForExit(subscriber);

        expect(finalState).toEqual(expect.objectContaining({ status: "completed", result: "done" }));
        expect((await readFile(`${dir}/charges.txt`, "utf8")).trim().split("\n")).toEqual(["job-1"]);
        expect((await readFile(`${dir}/second-attempts.txt`, "utf8")).trim().split("\n")).toEqual([
          String(ownerInfo.pid),
          String(subscriberInfo.pid),
        ]);
      } finally {
        if (owner.exitCode === null && owner.signalCode === null) owner.kill("SIGKILL");
        if (subscriber && subscriber.exitCode === null && subscriber.signalCode === null) subscriber.kill("SIGKILL");
      }
    });
  });

  it("rejects conflicting keyed inputs across independent engines", async () => {
    await withTempDir(async (dir) => {
      const gate = deferred<void>();
      const wf = workflow<{ value: number }>()(async (ctx, input) =>
        ctx.step(async () => {
          await gate.promise;
          return input.value;
        }, { name: "effect" })
      );
      const first = createEngine({ storage: new FileStorage(dir) });
      const second = createEngine({ storage: new FileStorage(dir) });
      await first.start();
      await second.start();
      await first.enqueue(wf, { value: 1 }, { key: "shared-key" });
      await tick();

      await expect(
        second.enqueue(wf, { value: 2 }, { key: "shared-key" }),
      ).rejects.toThrow(/key|conflict/i);
      gate.resolve();
      await first.drain();
      await first.stop();
      await second.stop();
    });
  });
});
