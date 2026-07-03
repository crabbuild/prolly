import Foundation

let packageDir = URL(fileURLWithPath: #filePath)
    .deletingLastPathComponent()
    .deletingLastPathComponent()
    .deletingLastPathComponent()

let tools = [
    "prolly-batch-build",
    "prolly-local-first-state",
    "prolly-resolver",
    "prolly-crdt-merge",
    "prolly-conversation-memory",
    "prolly-agent-event-log",
    "prolly-background-compaction",
    "prolly-deterministic-rag-snapshot",
    "prolly-document-chunk-index",
    "prolly-vector-sidecar",
    "prolly-provenance-values",
    "prolly-materialized-view",
    "prolly-filesystem-snapshot",
    "prolly-durable-sqlite",
]

for tool in tools {
    let process = Process()
    process.currentDirectoryURL = packageDir
    process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
    process.arguments = ["swift", "run", tool]
    try process.run()
    process.waitUntilExit()
    precondition(process.terminationStatus == 0, "\(tool) failed")
}
