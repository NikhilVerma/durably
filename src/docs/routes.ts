const readme = "https://www.npmjs.com/package/@nikhilverma/durably";

export const docsRoutes = {
  loop: `${readme}#docs-loop`,
  fanout: `${readme}#docs-fanout`,
  resources: `${readme}#docs-resources`,
  stash: `${readme}#docs-stash`,
  budgets: `${readme}#docs-budgets`,
  "wait-for": `${readme}#docs-wait-for`,
  "loop#snapshot-size": `${readme}#snapshot-size`,
  timeouts: `${readme}#docs-timeouts`,
  validation: `${readme}#docs-validation`,
  serialization: `${readme}#docs-serialization`,
  purity: `${readme}#docs-purity`,
  staleness: `${readme}#docs-staleness`,
  keys: `${readme}#docs-keys`,
  cancellation: `${readme}#docs-cancellation`,
  leases: `${readme}#docs-leases`,
} as const;

export type DocsRoute = keyof typeof docsRoutes;
