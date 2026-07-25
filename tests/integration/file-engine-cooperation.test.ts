import { describe, expect, it } from "bun:test";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { FileStorage, createEngine, workflow } from "../../index.ts";
import {
  deferred,
  findFilesNamed,
  objectWithStringIdSchema,
  tick,
  withTempDir,
} from "../support/kit.ts";

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

  it("heartbeats a long-running step so a rival engine cannot steal its lease", async () => {
    await withTempDir(async (dir) => {
      const gate = deferred<void>();
      let effects = 0;
      const wf = workflow({ name: "long-step" }, async (ctx) =>
        ctx.step(async () => {
          effects++;
          await gate.promise;
          return "charged";
        }, { name: "charge" })
      );
      const owner = createEngine({ storage: new FileStorage(dir), leaseMs: 200 });
      const rival = createEngine({ storage: new FileStorage(dir), leaseMs: 200 });
      await owner.start();
      const handle = await owner.enqueue(wf, undefined, { key: "long" });
      await tick();
      expect(effects).toBe(1);

      await rival.start();
      await rival.enqueue(wf, undefined, { key: "long" });
      await new Promise((resolve) => setTimeout(resolve, 500));

      expect(effects).toBe(1);
      gate.resolve();
      await Promise.all([owner.drain(), rival.drain()]);

      expect(effects).toBe(1);
      expect(await owner.inspect(handle.runId)).toEqual(expect.objectContaining({
        status: "completed",
        result: "charged",
      }));
      await owner.stop();
      await rival.stop();
    });
  }, 15_000);

  it("a stale lease owner discards its work instead of overwriting the new owner's state", async () => {
    await withTempDir(async (dir) => {
      const gate = deferred<void>();
      let attempts = 0;
      const wf = workflow({ name: "stealable" }, async (ctx) =>
        ctx.step(async () => {
          const attempt = ++attempts;
          if (attempt === 1) await gate.promise;
          return attempt;
        }, { name: "charge" })
      );
      const stale = createEngine({
        storage: new FileStorage(dir),
        leaseMs: 30,
        heartbeatMs: 10_000,
      });
      await stale.start();
      const handle = await stale.enqueue(wf, undefined, { key: "stolen" });
      await tick();
      expect(attempts).toBe(1);

      await new Promise((resolve) => setTimeout(resolve, 80));
      const reclaimer = createEngine({
        storage: new FileStorage(dir),
        leaseMs: 30,
        heartbeatMs: 10_000,
      });
      await reclaimer.start();
      await reclaimer.drain();
      expect(await reclaimer.inspect(handle.runId)).toEqual(expect.objectContaining({
        status: "completed",
        result: 2,
      }));

      gate.resolve();
      await stale.drain();

      expect(await reclaimer.inspect(handle.runId)).toEqual(expect.objectContaining({
        status: "completed",
        result: 2,
      }));
      expect((await stale.inspect(handle.runId))?.result).toBe(2);
      expect(attempts).toBe(2);
      await stale.stop();
      await reclaimer.stop();
    });
  }, 15_000);

  it("stays parked when another engine delivers a payload the wait rejects", async () => {
    await withTempDir(async (dir) => {
      const wf = workflow({ name: "approval-gate" }, async (ctx) =>
        ctx.waitFor("approval", objectWithStringIdSchema)
      );
      const parker = createEngine({ storage: new FileStorage(dir) });
      await parker.start();
      const handle = await parker.enqueue(wf, undefined);
      await parker.drain();
      expect((await parker.inspect(handle.runId))?.status).toBe("waiting");
      await parker.stop();

      // This engine never executed the workflow body, so it cannot hold the
      // wait's schema: the payload can only be checked where it is consumed.
      const signaller = createEngine({ storage: new FileStorage(dir) });
      await signaller.start();
      await signaller.signal(handle.runId, "approval", { id: 42 });
      await signaller.drain();
      expect((await signaller.inspect(handle.runId))?.status).toBe("waiting");

      await signaller.signal(handle.runId, "approval", { id: "operator-1" });
      await signaller.drain();
      expect(await signaller.inspect(handle.runId)).toEqual(expect.objectContaining({
        status: "completed",
        result: { id: "operator-1" },
      }));
      await signaller.stop();
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
