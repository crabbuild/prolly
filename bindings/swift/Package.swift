// swift-tools-version: 5.10

import PackageDescription

let localLibrarySearchPath =
    Context.environment["PROLLY_BINDINGS_LIBRARY_DIR"] ?? "../../target/debug"

let package = Package(
    name: "Prolly",
    platforms: [
        .macOS(.v13),
        .iOS(.v15),
    ],
    products: [
        .library(name: "Prolly", targets: ["Prolly"]),
        .library(name: "ProllyAPI", targets: ["ProllyAPI"]),
        .library(name: "ProllyStoreSQLite", targets: ["ProllyStoreSQLite"]),
        .library(name: "ProllyStorePostgres", targets: ["ProllyStorePostgres"]),
        .library(name: "ProllyStoreMySQL", targets: ["ProllyStoreMySQL"]),
        .library(name: "ProllyStoreRedis", targets: ["ProllyStoreRedis"]),
        .executable(name: "prolly-store-sqlite-check", targets: ["StoreSQLiteCheck"]),
        .executable(name: "prolly-store-postgres-check", targets: ["StorePostgresCheck"]),
        .executable(name: "prolly-store-mysql-check", targets: ["StoreMySQLCheck"]),
        .executable(name: "prolly-store-redis-check", targets: ["StoreRedisCheck"]),
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
    dependencies: [
        .package(url: "https://github.com/vapor/postgres-nio.git", exact: "1.27.0"),
        // PostgresNIO 1.27 supports Swift 5.10; constrain this transitive
        // package because newer releases require the Swift 6 plugin toolchain.
        .package(url: "https://github.com/apple/swift-async-algorithms.git", exact: "1.0.0"),
        .package(url: "https://github.com/apple/swift-log.git", exact: "1.6.4"),
        .package(url: "https://github.com/apple/swift-nio.git", exact: "2.86.2"),
        .package(url: "https://github.com/vapor/mysql-nio.git", exact: "1.8.0"),
        // 1.6.2 is the newest release whose package manifest supports Swift 5.10.
        .package(url: "https://github.com/swift-server/RediStack.git", exact: "1.6.2"),
    ],
    targets: [
        .systemLibrary(name: "CSQLite", pkgConfig: "sqlite3"),
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
            name: "ProllyAPI",
            dependencies: ["Prolly", "prollyFFI"]
        ),
        .target(
            name: "ProllyStoreSQLite",
            dependencies: ["Prolly", "CSQLite"],
            exclude: ["README.md"]
        ),
        .target(
            name: "ProllyStorePostgres",
            dependencies: [
                "Prolly",
                // This explicit product keeps the Swift 5.10-compatible
                // transitive version constraint active in clean resolves.
                .product(name: "AsyncAlgorithms", package: "swift-async-algorithms"),
                .product(name: "PostgresNIO", package: "postgres-nio"),
                .product(name: "Logging", package: "swift-log"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "ProllyStoreMySQL",
            dependencies: [
                "Prolly",
                .product(name: "MySQLNIO", package: "mysql-nio"),
                .product(name: "NIOCore", package: "swift-nio"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "ProllyStoreRedis",
            dependencies: ["Prolly", .product(name: "RediStack", package: "RediStack")],
            exclude: ["README.md"]
        ),
        .executableTarget(
            name: "StoreSQLiteCheck",
            dependencies: ["Prolly", "ProllyStoreSQLite", "CSQLite"],
            path: "Examples/StoreSQLiteCheck"
        ),
        .executableTarget(
            name: "StorePostgresCheck",
            dependencies: ["Prolly", "ProllyStorePostgres", .product(name: "PostgresNIO", package: "postgres-nio")],
            path: "Examples/StorePostgresCheck"
        ),
        .executableTarget(
            name: "StoreMySQLCheck",
            dependencies: [
                "Prolly", "ProllyStoreMySQL",
                .product(name: "MySQLNIO", package: "mysql-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
            ],
            path: "Examples/StoreMySQLCheck"
        ),
        .executableTarget(
            name: "StoreRedisCheck",
            dependencies: [
                "Prolly", "ProllyStoreRedis",
                .product(name: "RediStack", package: "RediStack"),
                .product(name: "NIOPosix", package: "swift-nio"),
            ],
            path: "Examples/StoreRedisCheck"
        ),
        .testTarget(
            name: "ProllyTests",
            dependencies: ["Prolly", "ProllyAPI"]
        ),
        .executableTarget(
            name: "AgentEventLog",
            dependencies: ["Prolly"],
            path: "Examples/AgentEventLog"
        ),
        .executableTarget(
            name: "BackgroundCompaction",
            dependencies: ["Prolly"],
            path: "Examples/BackgroundCompaction"
        ),
        .executableTarget(
            name: "BasicMap",
            dependencies: ["Prolly"],
            path: "Examples/BasicMap"
        ),
        .executableTarget(
            name: "BatchBuild",
            dependencies: ["Prolly"],
            path: "Examples/BatchBuild"
        ),
        .executableTarget(
            name: "ConversationMemory",
            dependencies: ["Prolly"],
            path: "Examples/ConversationMemory"
        ),
        .executableTarget(
            name: "CookbookScenarios",
            dependencies: ["Prolly"],
            path: "Examples/CookbookScenarios"
        ),
        .executableTarget(
            name: "CrdtMerge",
            dependencies: ["Prolly"],
            path: "Examples/CrdtMerge"
        ),
        .executableTarget(
            name: "DeterministicRagSnapshot",
            dependencies: ["Prolly"],
            path: "Examples/DeterministicRagSnapshot"
        ),
        .executableTarget(
            name: "DiffMerge",
            dependencies: ["Prolly"],
            path: "Examples/DiffMerge"
        ),
        .executableTarget(
            name: "DocumentChunkIndex",
            dependencies: ["Prolly"],
            path: "Examples/DocumentChunkIndex"
        ),
        .executableTarget(
            name: "DurableSqlite",
            dependencies: ["Prolly"],
            path: "Examples/DurableSqlite"
        ),
        .executableTarget(
            name: "FileBlobStore",
            dependencies: ["Prolly"],
            path: "Examples/FileBlobStore"
        ),
        .executableTarget(
            name: "FilesystemSnapshot",
            dependencies: ["Prolly"],
            path: "Examples/FilesystemSnapshot"
        ),
        .executableTarget(
            name: "LocalFirstState",
            dependencies: ["Prolly"],
            path: "Examples/LocalFirstState"
        ),
        .executableTarget(
            name: "MaterializedView",
            dependencies: ["Prolly"],
            path: "Examples/MaterializedView"
        ),
        .executableTarget(
            name: "ProvenanceValues",
            dependencies: ["Prolly"],
            path: "Examples/ProvenanceValues"
        ),
        .executableTarget(
            name: "Resolver",
            dependencies: ["Prolly"],
            path: "Examples/Resolver"
        ),
        .executableTarget(
            name: "SecondaryIndex",
            dependencies: ["Prolly"],
            path: "Examples/SecondaryIndex"
        ),
        .executableTarget(
            name: "VectorSidecar",
            dependencies: ["Prolly"],
            path: "Examples/VectorSidecar"
        ),
        .executableTarget(
            name: "FixtureCheck",
            dependencies: ["Prolly"],
            path: "Examples/FixtureCheck"
        ),
    ]
)
