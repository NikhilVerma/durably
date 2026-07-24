import { createRequire } from "node:module";
import { join } from "node:path";
import {
  FileStorage,
  type RunClaim,
  type StorageAdapter,
  type StorageListOptions,
  type StoredRun,
} from "./index.ts";

interface DatabaseLike {
  exec(sql: string): void;
  prepare(sql: string): {
    get(...values: unknown[]): unknown;
    run(...values: unknown[]): unknown;
  };
}

/**
 * The SQLite entrypoint is intentionally isolated from the core module. Run
 * events use the same hardened adapter contract; SQLite stores shared resource
 * state transactionally while the event log remains inspectable beside the DB.
 */
export class SqliteStorage implements StorageAdapter {
  readonly #database: DatabaseLike | undefined;
  readonly #events: FileStorage;

  constructor(path: string) {
    this.#events = new FileStorage(`${path}.durably`);
    try {
      const sqlite = createRequire(join(process.cwd(), "__durably_sqlite__.js"))("node:sqlite") as {
        DatabaseSync: new (databasePath: string) => DatabaseLike;
      };
      this.#database = new sqlite.DatabaseSync(path);
      this.#database?.exec(`
        CREATE TABLE IF NOT EXISTS durably_resources (
          name TEXT PRIMARY KEY,
          state TEXT NOT NULL
        )
      `);
    } catch {
      // Node <22.5 and some Bun builds do not expose node:sqlite. The adapter's
      // event log remains usable; resource state falls back to its sidecar.
      this.#database = undefined;
    }
  }

  init(): Promise<void> {
    return this.#events.init();
  }
  createRun(run: StoredRun): Promise<void> {
    return this.#events.createRun(run);
  }
  append(runId: string, event: unknown): Promise<void> {
    return this.#events.append(runId, event);
  }
  read(runId: string): Promise<unknown[] | null> {
    return this.#events.read(runId);
  }
  claim(runId: string, claim: RunClaim): Promise<boolean> {
    return this.#events.claim(runId, claim);
  }
  heartbeat(runId: string, claim: RunClaim): Promise<boolean> {
    return this.#events.heartbeat(runId, claim);
  }
  list(options?: StorageListOptions): Promise<StoredRun[]> {
    return this.#events.list(options);
  }
  release(runId: string, owner: string): Promise<void> {
    return this.#events.release(runId, owner);
  }
  async loadResourceState(name: string): Promise<unknown> {
    if (!this.#database) return this.#events.loadResourceState(name);
    const row = this.#database
      .prepare("SELECT state FROM durably_resources WHERE name = ?")
      .get(name) as { state?: string } | undefined;
    return row?.state ? JSON.parse(row.state) : undefined;
  }
  async saveResourceState(name: string, state: unknown): Promise<void> {
    if (!this.#database) return this.#events.saveResourceState(name, state);
    this.#database.prepare(`
      INSERT INTO durably_resources(name, state) VALUES (?, ?)
      ON CONFLICT(name) DO UPDATE SET state = excluded.state
    `).run(name, JSON.stringify(state));
  }
}
