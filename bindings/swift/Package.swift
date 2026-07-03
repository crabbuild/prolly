// swift-tools-version: 5.10

import PackageDescription

let localLibrarySearchPath =
    Context.environment["PROLLY_BINDINGS_LIBRARY_DIR"] ?? "../../../../target/debug"

let package = Package(
    name: "Prolly",
    platforms: [
        .macOS(.v13),
        .iOS(.v15),
    ],
    products: [
        .library(name: "Prolly", targets: ["Prolly"]),
        .executable(name: "prolly-agent-event-log", targets: ["AgentEventLog"]),
        .executable(name: "prolly-background-compaction", targets: ["BackgroundCompaction"]),
        .executable(name: "prolly-basic-map", targets: ["BasicMap"]),
        .executable(name: "prolly-batch-build", targets: ["BatchBuild"]),
        .executable(name: "prolly-conversation-memory", targets: ["ConversationMemory"]),
        .executable(name: "prolly-cookbook-scenarios", targets: ["CookbookScenarios"]),
        .executable(name: "prolly-crdt-merge", targets: ["CrdtMerge"]),
        .executable(name: "prolly-deterministic-rag-snapshot", targets: ["DeterministicRagSnapshot"]),
        .executable(name: "prolly-diff-merge", targets: ["DiffMerge"]),
        .executable(name: "prolly-document-chunk-index", targets: ["DocumentChunkIndex"]),
        .executable(name: "prolly-durable-sqlite", targets: ["DurableSqlite"]),
        .executable(name: "prolly-file-blob-store", targets: ["FileBlobStore"]),
        .executable(name: "prolly-filesystem-snapshot", targets: ["FilesystemSnapshot"]),
        .executable(name: "prolly-local-first-state", targets: ["LocalFirstState"]),
        .executable(name: "prolly-materialized-view", targets: ["MaterializedView"]),
        .executable(name: "prolly-provenance-values", targets: ["ProvenanceValues"]),
        .executable(name: "prolly-resolver", targets: ["Resolver"]),
        .executable(name: "prolly-secondary-index", targets: ["SecondaryIndex"]),
        .executable(name: "prolly-vector-sidecar", targets: ["VectorSidecar"]),
        .executable(name: "prolly-fixture-check", targets: ["FixtureCheck"]),
    ],
    targets: [
        .target(
            name: "prollyFFI",
            publicHeadersPath: "include"
        ),
        .target(
            name: "Prolly",
            dependencies: ["prollyFFI"],
            exclude: ["PROVENANCE.md"],
            linkerSettings: [
                .unsafeFlags(["-L\(localLibrarySearchPath)"]),
                .linkedLibrary("prolly_bindings"),
            ]
        ),
        .target(
            name: "CookbookSupport",
            dependencies: ["Prolly"],
            path: "Examples/CookbookSupport"
        ),
        .executableTarget(
            name: "AgentEventLog",
            dependencies: ["CookbookSupport"],
            path: "Examples/AgentEventLog"
        ),
        .executableTarget(
            name: "BackgroundCompaction",
            dependencies: ["CookbookSupport"],
            path: "Examples/BackgroundCompaction"
        ),
        .executableTarget(
            name: "BasicMap",
            dependencies: ["Prolly"],
            path: "Examples/BasicMap"
        ),
        .executableTarget(
            name: "BatchBuild",
            dependencies: ["CookbookSupport"],
            path: "Examples/BatchBuild"
        ),
        .executableTarget(
            name: "ConversationMemory",
            dependencies: ["CookbookSupport"],
            path: "Examples/ConversationMemory"
        ),
        .executableTarget(
            name: "CookbookScenarios",
            dependencies: ["CookbookSupport"],
            path: "Examples/CookbookScenarios"
        ),
        .executableTarget(
            name: "CrdtMerge",
            dependencies: ["CookbookSupport"],
            path: "Examples/CrdtMerge"
        ),
        .executableTarget(
            name: "DeterministicRagSnapshot",
            dependencies: ["CookbookSupport"],
            path: "Examples/DeterministicRagSnapshot"
        ),
        .executableTarget(
            name: "DiffMerge",
            dependencies: ["Prolly"],
            path: "Examples/DiffMerge"
        ),
        .executableTarget(
            name: "DocumentChunkIndex",
            dependencies: ["CookbookSupport"],
            path: "Examples/DocumentChunkIndex"
        ),
        .executableTarget(
            name: "DurableSqlite",
            dependencies: ["CookbookSupport"],
            path: "Examples/DurableSqlite"
        ),
        .executableTarget(
            name: "FileBlobStore",
            dependencies: ["Prolly"],
            path: "Examples/FileBlobStore"
        ),
        .executableTarget(
            name: "FilesystemSnapshot",
            dependencies: ["CookbookSupport"],
            path: "Examples/FilesystemSnapshot"
        ),
        .executableTarget(
            name: "LocalFirstState",
            dependencies: ["CookbookSupport"],
            path: "Examples/LocalFirstState"
        ),
        .executableTarget(
            name: "MaterializedView",
            dependencies: ["CookbookSupport"],
            path: "Examples/MaterializedView"
        ),
        .executableTarget(
            name: "ProvenanceValues",
            dependencies: ["CookbookSupport"],
            path: "Examples/ProvenanceValues"
        ),
        .executableTarget(
            name: "Resolver",
            dependencies: ["CookbookSupport"],
            path: "Examples/Resolver"
        ),
        .executableTarget(
            name: "SecondaryIndex",
            dependencies: ["Prolly"],
            path: "Examples/SecondaryIndex"
        ),
        .executableTarget(
            name: "VectorSidecar",
            dependencies: ["CookbookSupport"],
            path: "Examples/VectorSidecar"
        ),
        .executableTarget(
            name: "FixtureCheck",
            dependencies: ["Prolly"],
            path: "Examples/FixtureCheck"
        ),
    ]
)
