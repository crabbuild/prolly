import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const scenarios = [
  "batch-build.ts",
  "local-first-state.ts",
  "resolver.ts",
  "conversation-memory.ts",
  "agent-event-log.ts",
  "background-compaction.ts",
  "deterministic-rag-snapshot.ts",
  "document-chunk-index.ts",
  "vector-sidecar.ts",
  "provenance-values.ts",
  "materialized-view.ts",
  "browser-storage.ts",
];

for (const scenario of scenarios) {
  const url = new URL(`./${scenario}`, import.meta.url);
  const result = spawnSync(process.execPath, [fileURLToPath(url)], { stdio: "inherit" });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}
