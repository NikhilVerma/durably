import { execFileSync } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const cache = await mkdtemp(join(tmpdir(), "durably-attw-cache-"));
try {
  execFileSync(
    join(process.cwd(), "node_modules", ".bin", "attw"),
    ["--pack"],
    {
      cwd: process.cwd(),
      env: { ...process.env, NPM_CONFIG_CACHE: cache },
      stdio: "inherit",
    },
  );
} finally {
  await rm(cache, { recursive: true, force: true });
}
