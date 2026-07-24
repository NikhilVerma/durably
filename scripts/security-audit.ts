import { readFile } from "node:fs/promises";
import { parseConfigFileTextToJson } from "typescript";

interface CommandResult {
  exitCode: number;
  output: string;
}

interface BunLock {
  packages?: Record<string, unknown>;
}

interface OsvResult {
  vulns?: Array<{ id: string }>;
}

const run = async (command: string[]): Promise<CommandResult> => {
  const process = Bun.spawn(command, {
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(process.stdout).text(),
    new Response(process.stderr).text(),
    process.exited,
  ]);
  const output = `${stdout}${stderr}`;
  return { exitCode, output };
};

const parseLockedPackages = async (): Promise<
  Array<{ name: string; version: string }>
> => {
  const source = await readFile("bun.lock", "utf8");
  const parsed = parseConfigFileTextToJson("bun.lock", source);
  if (parsed.error) {
    throw new Error("Could not parse bun.lock for the OSV fallback");
  }

  const lock = parsed.config as BunLock;
  const packages = new Map<string, { name: string; version: string }>();
  for (const value of Object.values(lock.packages ?? {})) {
    if (!Array.isArray(value) || typeof value[0] !== "string") continue;
    const identifier = value[0];
    const separator = identifier.lastIndexOf("@");
    if (separator <= 0 || separator === identifier.length - 1) {
      throw new Error(`Unsupported bun.lock package identifier: ${identifier}`);
    }
    const name = identifier.slice(0, separator);
    const version = identifier.slice(separator + 1);
    packages.set(`${name}@${version}`, { name, version });
  }
  return [...packages.values()];
};

const auditWithOsv = async (): Promise<void> => {
  const packages = await parseLockedPackages();
  const response = await fetch("https://api.osv.dev/v1/querybatch", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      queries: packages.map(({ name, version }) => ({
        package: { ecosystem: "npm", name },
        version,
      })),
    }),
  });
  if (!response.ok) {
    throw new Error(`OSV audit request failed (status ${response.status})`);
  }

  const payload = (await response.json()) as { results?: OsvResult[] };
  if (!Array.isArray(payload.results) || payload.results.length !== packages.length) {
    throw new Error("OSV audit returned an incomplete result set");
  }

  const findings = payload.results.flatMap((result, index) =>
    (result.vulns ?? []).map((vulnerability) => ({
      package: `${packages[index]!.name}@${packages[index]!.version}`,
      id: vulnerability.id,
    }))
  );
  if (findings.length > 0) {
    for (const finding of findings) {
      console.error(`${finding.package}: ${finding.id}`);
    }
    throw new Error(`OSV found ${findings.length} known vulnerability record(s)`);
  }
  console.log(`OSV found no known vulnerabilities in ${packages.length} locked packages`);
};

const forceOsv = process.argv.includes("--osv");
if (!forceOsv) {
  const bunAudit = await run([process.execPath, "audit"]);
  process.stdout.write(bunAudit.output);
  if (bunAudit.exitCode === 0) {
    process.exit(0);
  }
  if (!/audit request failed \(status 5\d\d\)/i.test(bunAudit.output)) {
    process.exit(bunAudit.exitCode);
  }
  console.warn("bun audit returned a transient 5xx; falling back to an exact bun.lock OSV scan");
}

await auditWithOsv();
