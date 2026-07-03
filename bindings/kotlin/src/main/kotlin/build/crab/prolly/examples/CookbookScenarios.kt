package build.crab.prolly.examples

import build.crab.prolly.CrdtDeletePolicyKind
import build.crab.prolly.DiffKind
import build.crab.prolly.EntryRecord
import build.crab.prolly.LargeValueConfigRecord
import build.crab.prolly.MutationKind
import build.crab.prolly.MutationRecord
import build.crab.prolly.ProllyBlobStore
import build.crab.prolly.ProllyEngine
import build.crab.prolly.ProllyNative
import build.crab.prolly.TimestampedValueRecord
import build.crab.prolly.TreeRecord
import build.crab.prolly.ValueRefKind
import build.crab.prolly.cidFromBytes
import build.crab.prolly.crdtConfigLww
import build.crab.prolly.defaultConfig
import build.crab.prolly.multiValueSetMerge
import build.crab.prolly.timestampedValueFromBytes
import build.crab.prolly.timestampedValueToBytes
import java.nio.file.Files

fun main() {
    ProllyNative.useLocalDebugLibrary()
    basicMap()
    diffMerge()
    fileBlobStore()
    secondaryIndex()
    batchBuild()
    localFirstState()
    resolver()
    crdtMerge()
    conversationMemory()
    agentEventLog()
    backgroundCompaction()
    deterministicRagSnapshot()
    documentChunkIndex()
    vectorSidecar()
    provenanceValues()
    materializedView()
    filesystemSnapshot()
    durableSqlite()
}

fun basicMap() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        var tree = engine.create()
        tree = engine.put(tree, bytes("user:001"), bytes("Ada"))
        tree = engine.put(tree, bytes("user:002"), bytes("Grace"))
        tree = engine.put(tree, bytes("user:003"), bytes("Linus"))

        requireBytes(bytes("Ada"), requireNotNull(engine.get(tree, bytes("user:001"))), "user:001")

        tree = engine.delete(tree, bytes("user:003"))
        require(engine.get(tree, bytes("user:003")) == null) { "user:003 should be deleted" }

        val users = engine.range(tree, bytes("user:"), bytes("user;"))
        require(users.size == 2) { "expected two users" }
        requireBytes(bytes("user:001"), users[0].key, "first key")
        requireBytes(bytes("Ada"), users[0].value, "first value")
        requireBytes(bytes("user:002"), users[1].key, "second key")
        requireBytes(bytes("Grace"), users[1].value, "second value")

        println("basic_map: ${users.size} users in range")
    }
}

fun diffMerge() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        var base = engine.create()
        base = engine.put(base, bytes("doc:title"), bytes("Draft"))

        val left = engine.put(base, bytes("doc:body"), bytes("Hello"))
        val right = engine.put(base, bytes("doc:tags"), bytes("example"))

        val leftChanges = engine.diff(base, left)
        require(leftChanges.size == 1) { "expected one left-side change" }
        requireBytes(bytes("doc:body"), leftChanges[0].key, "diff key")

        val merged = engine.merge(base, left, right, "prefer_right")
        requireBytes(bytes("Hello"), requireNotNull(engine.get(merged, bytes("doc:body"))), "merged body")
        requireBytes(bytes("example"), requireNotNull(engine.get(merged, bytes("doc:tags"))), "merged tags")

        println("diff_merge: merged ${leftChanges.size} left-side change")
    }
}

fun fileBlobStore() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        ProllyBlobStore.memory().use { blobStore ->
            var tree = engine.create()
            tree = engine.putLargeValue(blobStore, tree, bytes("doc/body"), ByteArray(64) { 7 }, LargeValueConfigRecord(8UL))
            require(requireNotNull(engine.getValueRef(tree, bytes("doc/body"))).kind == ValueRefKind.BLOB) {
                "expected blob value ref"
            }

            val updated =
                engine.putLargeValue(blobStore, tree, bytes("doc/body"), ByteArray(64) { 9 }, LargeValueConfigRecord(8UL))
            requireBytes(ByteArray(64) { 9 }, requireNotNull(engine.getLargeValue(blobStore, updated, bytes("doc/body"))), "large value")

            val plan = engine.planBlobStoreGc(blobStore, listOf(updated))
            require(plan.reclaimableBlobCount == 1UL) { "expected one reclaimable blob" }
            val sweep = engine.sweepBlobStoreGc(blobStore, listOf(updated))
            require(sweep.deletedBlobs == 1UL) { "expected one deleted blob" }

            println("file_blob_store: reclaimed ${sweep.deletedBlobBytes} bytes")
        }
    }
}

fun largeValues() {
    fileBlobStore()
}

fun secondaryIndex() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        val empty = engine.create()

        var sourceV1 = putUser(engine, empty, User("acme", "u001", "active", "Ada"))
        sourceV1 = putUser(engine, sourceV1, User("acme", "u002", "invited", "Grace"))
        val indexV1 = buildStatusIndex(engine, sourceV1)

        var sourceV2 = putUser(engine, sourceV1, User("acme", "u002", "active", "Grace"))
        sourceV2 = putUser(engine, sourceV2, User("globex", "u003", "active", "Linus"))

        val sourceChanges = engine.diff(sourceV1, sourceV2)
        require(sourceChanges.size == 2) { "expected two source changes" }

        val indexV2 = applySourceDiff(engine, indexV1, sourceChanges)
        val rebuiltIndexV2 = buildStatusIndex(engine, sourceV2)
        require(indexV2.root?.contentEquals(rebuiltIndexV2.root) == true) { "incremental index does not match rebuilt index" }

        require(usersByStatus(engine, indexV2, "acme", "active").size == 2) { "expected two acme active users" }
        require(usersByStatus(engine, indexV2, "acme", "invited").isEmpty()) { "expected no acme invited users" }
        require(usersByStatus(engine, indexV2, "globex", "active").size == 1) { "expected one globex active user" }

        println("secondary_index: applied ${sourceChanges.size} source diffs")
    }
}

fun batchBuild() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        val entries =
            (64 downTo 1).map { index ->
                EntryRecord(bytes("event/%04d".format(index)), bytes("payload-$index"))
            }
        val tree = engine.buildFromEntries(entries)
        val rows = engine.range(tree, bytes("event/"), bytes("event0"))
        val stats = engine.collectStatsJson(tree).json

        require(rows.size == 64) { "expected 64 rows" }
        requireBytes(bytes("event/0001"), rows.first().key, "first event key")
        require(stats.contains("num_nodes")) { "stats should include num_nodes" }

        println("batch_build: imported ${rows.size} events")
    }
}

fun localFirstState() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        val main = bytes("app/demo/root/main")
        val base =
            engine.batch(
                engine.create(),
                listOf(
                    upsert("entity/user/001", bytes("Ada")),
                    upsert("index/user/name/Ada/001", ByteArray(0)),
                ),
            )
        engine.publishNamedRoot(main, base)

        val device =
            engine.batch(
                base,
                listOf(
                    upsert("entity/task/900", bytes("offline draft")),
                    upsert("index/task/status/open/900", ByteArray(0)),
                ),
            )
        val canonical = engine.put(base, bytes("entity/user/002"), bytes("Grace"))
        engine.publishNamedRoot(main, canonical)

        val current = requireNotNull(engine.loadNamedRoot(main))
        val merged = engine.merge(base, current, device, "prefer_right")
        val update = engine.compareAndSwapNamedRoot(main, current, merged)

        require(update.applied) { "main root CAS failed" }
        requireBytes(bytes("Grace"), requireNotNull(engine.get(merged, bytes("entity/user/002"))), "canonical user")
        requireBytes(bytes("offline draft"), requireNotNull(engine.get(merged, bytes("entity/task/900"))), "device task")

        println("local_first_state: merged offline branch into main")
    }
}

fun resolver() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        val base = engine.put(engine.create(), bytes("settings/theme"), bytes("light"))
        val leftDelete = engine.delete(base, bytes("settings/theme"))
        val rightUpdate = engine.put(base, bytes("settings/theme"), bytes("dark"))

        val updateWins = engine.merge(base, leftDelete, rightUpdate, "update_wins")
        val deleteWins = engine.merge(base, leftDelete, rightUpdate, "delete_wins")

        requireBytes(bytes("dark"), requireNotNull(engine.get(updateWins, bytes("settings/theme"))), "update-wins setting")
        require(engine.get(deleteWins, bytes("settings/theme")) == null) { "delete-wins should remove setting" }

        println("resolver: demonstrated update-wins and delete-wins policies")
    }
}

fun crdtMerge() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        val baseValue = timestampedValueToBytes(TimestampedValueRecord(bytes("base"), 1UL))
        val leftValue = timestampedValueToBytes(TimestampedValueRecord(bytes("left"), 2UL))
        val rightValue = timestampedValueToBytes(TimestampedValueRecord(bytes("right"), 3UL))

        val base = engine.put(engine.create(), bytes("counter/global"), baseValue)
        val left = engine.put(base, bytes("counter/global"), leftValue)
        val right = engine.put(base, bytes("counter/global"), rightValue)
        val merged = engine.crdtMerge(base, left, right, crdtConfigLww(CrdtDeletePolicyKind.UPDATE_WINS))
        val decoded = timestampedValueFromBytes(requireNotNull(engine.get(merged, bytes("counter/global"))))
        val mergedSet = multiValueSetMerge(listOf(bytes("candidate-b")), listOf(bytes("candidate-a"), bytes("candidate-b")))

        requireBytes(bytes("right"), decoded.value, "CRDT value")
        require(decoded.timestamp == 3UL) { "CRDT timestamp mismatch" }
        requireBytes(bytes("candidate-a"), mergedSet[0], "first multi-value item")

        println("crdt_merge: last-writer-wins and multi-value helpers passed")
    }
}

fun conversationMemory() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        val main = bytes("conversation/c42/root/main")
        val attemptName = bytes("conversation/c42/attempt/extractor/a1")
        val base = engine.put(engine.create(), bytes("conversation/c42/memory/m001"), bytes("user|likes terse summaries|0.91"))
        engine.publishNamedRoot(main, base)
        val attempt = engine.put(base, bytes("conversation/c42/memory/m002"), bytes("user|uses Kotlin|0.87"))
        engine.publishNamedRoot(attemptName, attempt)
        val canonical = engine.put(base, bytes("conversation/c42/memory/m003"), bytes("user|prefers local-first apps|0.82"))
        engine.publishNamedRoot(main, canonical)

        val merged = engine.merge(
            base,
            requireNotNull(engine.loadNamedRoot(main)),
            requireNotNull(engine.loadNamedRoot(attemptName)),
            "prefer_right",
        )
        val update = engine.compareAndSwapNamedRoot(main, canonical, merged)
        val rows = engine.range(merged, bytes("conversation/c42/memory/"), bytes("conversation/c42/memory0"))

        require(update.applied) { "main root CAS failed" }
        require(rows.size == 3) { "expected three memories" }

        println("conversation_memory: accepted extractor attempt into canonical memory")
    }
}

fun agentEventLog() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        val root = bytes("agent-log/run-7/root/events/current")
        val tree =
            engine.batch(
                engine.create(),
                listOf(
                    upsert("agent-log/run-7/event/1783036805000/0001", bytes("user|Summarize the plan")),
                    upsert("agent-log/run-7/event/1783036805000/0002", bytes("tool-call|search-docs")),
                    upsert("agent-log/run-7/event/1783036806000/0003", bytes("assistant|Plan ready")),
                ),
            )
        engine.publishNamedRoot(root, tree)

        val page = engine.rangePage(requireNotNull(engine.loadNamedRoot(root)), null, null, 2UL)
        require(page.entries.size == 2) { "expected first event page" }
        require(page.nextCursor != null) { "expected next cursor" }

        println("agent_event_log: first page has ${page.entries.size} events")
    }
}

fun backgroundCompaction() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        val events =
            engine.batch(
                engine.create(),
                (1..6).map { index -> upsert("event/%04d".format(index), bytes("raw-event-$index")) },
            )
        engine.publishNamedRoot(bytes("compaction/run/r7/root/events/0001"), events)
        val compacted =
            engine.batch(
                events,
                listOf(
                    deleteMutation("event/0001"),
                    deleteMutation("event/0002"),
                    deleteMutation("event/0003"),
                    deleteMutation("event/0004"),
                    upsert("event/0004-summary", bytes("summary of events 1..4")),
                ),
            )
        engine.publishNamedRoot(bytes("compaction/run/r7/root/events/current"), compacted)

        val plan = engine.planStoreGc(listOf(events, compacted))
        val remaining = engine.range(compacted, bytes("event/"), bytes("event0"))
        require(remaining.size == 3) { "expected compacted log records" }
        require(plan.reclaimableNodes >= 0UL) { "invalid GC plan" }

        println("background_compaction: compacted log to ${remaining.size} records")
    }
}

fun deterministicRagSnapshot() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        val indexRoot = bytes("rag/corpus/docs/root/index/current")
        val indexV1 =
            engine.batch(
                engine.create(),
                listOf(
                    upsert("rag/corpus/docs/chunk/doc-1/0001", bytes("vector:v1|CrabDB stores deterministic roots")),
                    upsert("rag/corpus/docs/chunk/doc-2/0001", bytes("vector:v2|Prolly trees diff by key")),
                ),
            )
        engine.publishNamedRoot(indexRoot, indexV1)
        val answers = engine.put(
            engine.create(),
            bytes("rag/answer/q1"),
            bytes("query:q1|snapshot:${requireNotNull(indexV1.root).toHex()}|citation:doc-1/0001"),
        )
        engine.publishNamedRoot(bytes("rag/corpus/docs/root/answers"), answers)

        val indexV2 = engine.put(indexV1, bytes("rag/corpus/docs/chunk/doc-3/0001"), bytes("vector:v3|New content"))
        engine.publishNamedRoot(indexRoot, indexV2)

        val replayRows = engine.range(indexV1, bytes("rag/corpus/docs/chunk/"), bytes("rag/corpus/docs/chunk0")).size
        val currentRows =
            engine.range(requireNotNull(engine.loadNamedRoot(indexRoot)), bytes("rag/corpus/docs/chunk/"), bytes("rag/corpus/docs/chunk0")).size
        require(replayRows == 2 && currentRows == 3) { "RAG snapshot validation failed" }

        println("deterministic_rag_snapshot: replay kept original index root")
    }
}

fun documentChunkIndex() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        ProllyBlobStore.memory().use { blobStore ->
            val textKey = bytes("doc-index/corpus/text/parser-v1/doc-1/chunk-0001")
            val metadataKey = bytes("doc-index/corpus/parser/parser-v1/document/doc-1/chunk/000000")
            var tree =
                engine.putLargeValue(
                    blobStore,
                    engine.create(),
                    textKey,
                    bytes("CrabDB stores large chunk text outside prolly leaves.".repeat(8)),
                    LargeValueConfigRecord(32UL),
                )
            tree = engine.put(tree, metadataKey, bytes("doc-1|chunk-0001|0|384|vector-0001"))

            require(engine.range(tree, bytes("doc-index/corpus/parser/"), bytes("doc-index/corpus/parser0")).size == 1) {
                "metadata missing"
            }
            require(requireNotNull(engine.getLargeValue(blobStore, tree, textKey)).decodeToString().startsWith("CrabDB stores")) {
                "chunk text missing"
            }

            println("document_chunk_index: metadata and blob-backed chunk text are linked")
        }
    }
}

fun vectorSidecar() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        val sidecar = mapOf("vec-1" to listOf(0.9, 0.1), "vec-2" to listOf(0.8, 0.2), "vec-stale" to listOf(1.0, 0.0))
        val tree =
            engine.batch(
                engine.create(),
                listOf(
                    upsert("vector-sidecar/corpus/docs/chunk/doc-1/0001", bytes("vec-1|doc-1|parser-v1")),
                    upsert("vector-sidecar/corpus/docs/chunk/doc-2/0001", bytes("vec-2|doc-2|parser-v1")),
                ),
            )
        val allowed =
            engine.range(tree, bytes("vector-sidecar/corpus/docs/chunk/"), bytes("vector-sidecar/corpus/docs/chunk0"))
                .map { it.value.decodeToString().split("|", limit = 2).first() }
                .toSet()
        val hits = sidecar.keys.sorted().filter { it in allowed }
        require(hits == listOf("vec-1", "vec-2")) { "unexpected sidecar hits" }

        println("vector_sidecar: filtered sidecar hits to ${hits.size} snapshot vectors")
    }
}

fun provenanceValues() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        val source = bytes("CrabDB language bindings design")
        val sourceCid = cidFromBytes(source).toHex()
        val chunkCid = cidFromBytes(source.copyOfRange(0, 16)).toHex()
        val tree =
            engine.batch(
                engine.create(),
                listOf(
                    upsert("provenance/chunk/file-1/chunk-1", bytes("source=$sourceCid|chunk=$chunkCid|parser=v1")),
                    upsert("provenance/claim/file-1/claim-1", bytes("CrabDB uses Rust-backed bindings|chunk=file-1/chunk-1")),
                ),
            )

        val claims = engine.range(tree, bytes("provenance/claim/file-1/"), bytes("provenance/claim/file-10"))
        require(claims.size == 1) { "expected one claim" }
        require(claims.first().value.decodeToString().contains("Rust-backed")) { "missing claim text" }

        println("provenance_values: claim links back to source and chunk CIDs")
    }
}

fun materializedView() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        val o1 = Order("acme", "o1", "paid", 1200)
        val o2 = Order("acme", "o2", "open", 500)
        val sourceV1 =
            engine.batch(
                engine.create(),
                listOf(
                    MutationRecord(MutationKind.UPSERT, orderKey(o1), encodeOrder(o1)),
                    MutationRecord(MutationKind.UPSERT, orderKey(o2), encodeOrder(o2)),
                ),
            )
        val paidO2 = Order("acme", "o2", "paid", 500)
        val sourceV2 = engine.put(sourceV1, orderKey(paidO2), encodeOrder(paidO2))
        val viewV2 = buildRevenueView(engine, sourceV2)

        requireBytes(bytes("1700"), requireNotNull(engine.get(viewV2, viewKey("acme", "paid"))), "paid revenue")
        require(engine.get(viewV2, viewKey("acme", "open")) == null) { "open revenue should be absent" }

        println("materialized_view: folded ${engine.diff(sourceV1, sourceV2).size} source diff")
    }
}

fun filesystemSnapshot() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        ProllyBlobStore.memory().use { blobStore ->
            var tree = engine.create()
            for ((path, contents) in mapOf("README.md" to "# Demo\n", "src/lib.rs" to "pub fn answer() -> u8 { 42 }\n")) {
                tree = engine.putLargeValue(blobStore, tree, bytes("path/$path"), bytes(contents), LargeValueConfigRecord(4UL))
            }
            engine.publishNamedRoot(bytes("refs/heads/main"), tree)
            val loaded = requireNotNull(engine.loadNamedRoot(bytes("refs/heads/main")))
            requireBytes(bytes("# Demo\n"), requireNotNull(engine.getLargeValue(blobStore, loaded, bytes("path/README.md"))), "README.md")

            println("filesystem_snapshot: published branch with blob-backed file contents")
        }
    }
}

fun durableSqlite() {
    val dir = Files.createTempDirectory("prolly-kotlin-")
    try {
        ProllyEngine.sqlite(dir.resolve("app.prolly.sqlite").toString(), defaultConfig()).use { engine ->
            val tree = engine.batch(
                engine.create(),
                listOf(upsert("user/1", bytes("Ada")), upsert("user/2", bytes("Grace"))),
            )
            engine.publishNamedRoot(bytes("users/main"), tree)
            val loaded = requireNotNull(engine.loadNamedRoot(bytes("users/main")))
            require(loaded.root?.contentEquals(tree.root) == true) { "loaded SQLite root mismatch" }
            requireBytes(bytes("Ada"), requireNotNull(engine.get(loaded, bytes("user/1"))), "sqlite user")
        }
    } finally {
        dir.toFile().deleteRecursively()
    }
    println("durable_sqlite: named root survived through SQLite store API")
}

private data class Order(val tenant: String, val id: String, val status: String, val cents: Int)

private fun orderKey(order: Order): ByteArray =
    bytes("orders/source/tenant/${order.tenant}/order/${order.id}")

private fun encodeOrder(order: Order): ByteArray =
    bytes("${order.tenant}|${order.id}|${order.status}|${order.cents}")

private fun decodeOrder(value: ByteArray): Order {
    val parts = value.decodeToString().split("|", limit = 4)
    return Order(parts[0], parts[1], parts[2], parts[3].toInt())
}

private fun viewKey(tenant: String, status: String): ByteArray =
    bytes("orders/view/by-status/tenant/$tenant/status/$status")

private fun buildRevenueView(engine: ProllyEngine, source: TreeRecord): TreeRecord {
    val totals = linkedMapOf<Pair<String, String>, Int>()
    for (entry in engine.range(source, bytes("orders/source/"), bytes("orders/source0"))) {
        val order = decodeOrder(entry.value)
        val key = order.tenant to order.status
        totals[key] = (totals[key] ?: 0) + order.cents
    }
    val mutations =
        totals.entries
            .sortedBy { (key, _) -> "${key.first}|${key.second}" }
            .map { (key, cents) -> MutationRecord(MutationKind.UPSERT, viewKey(key.first, key.second), cents.toString().encodeToByteArray()) }
    return engine.batch(engine.create(), mutations)
}

private data class User(val tenant: String, val id: String, val status: String, val name: String)

private fun userKey(user: User): ByteArray =
    bytes("source/tenant/${user.tenant}/user/${user.id}")

private fun encodeUser(user: User): ByteArray =
    bytes(listOf(user.tenant, user.id, user.status, user.name).joinToString("|"))

private fun decodeUser(value: ByteArray): User {
    val parts = value.decodeToString().split("|", limit = 4)
    return User(parts[0], parts[1], parts[2], parts[3])
}

private fun statusIndexPrefix(tenant: String, status: String): ByteArray =
    bytes("index/user-by-status/tenant/$tenant/status/$status/")

private fun statusIndexKey(user: User): ByteArray =
    statusIndexPrefix(user.tenant, user.status) + bytes(user.id)

private fun putUser(engine: ProllyEngine, tree: build.crab.prolly.TreeRecord, user: User): build.crab.prolly.TreeRecord =
    engine.put(tree, userKey(user), encodeUser(user))

private fun buildStatusIndex(engine: ProllyEngine, source: build.crab.prolly.TreeRecord): build.crab.prolly.TreeRecord {
    var index = engine.create()
    for (entry in engine.range(source, bytes("source/"), bytes("source0"))) {
        index = engine.put(index, statusIndexKey(decodeUser(entry.value)), bytes("1"))
    }
    return index
}

private fun applySourceDiff(
    engine: ProllyEngine,
    index: build.crab.prolly.TreeRecord,
    changes: List<build.crab.prolly.DiffRecord>,
): build.crab.prolly.TreeRecord {
    var next = index
    for (change in changes) {
        when (change.kind) {
            DiffKind.ADDED -> next = engine.put(next, statusIndexKey(decodeUser(requireNotNull(change.value))), bytes("1"))
            DiffKind.REMOVED -> next = engine.delete(next, statusIndexKey(decodeUser(requireNotNull(change.value))))
            DiffKind.CHANGED -> {
                val oldKey = statusIndexKey(decodeUser(requireNotNull(change.oldValue)))
                val newKey = statusIndexKey(decodeUser(requireNotNull(change.newValue)))
                if (!oldKey.contentEquals(newKey)) {
                    next = engine.delete(next, oldKey)
                    next = engine.put(next, newKey, bytes("1"))
                }
            }
        }
    }
    return next
}

private fun usersByStatus(
    engine: ProllyEngine,
    index: build.crab.prolly.TreeRecord,
    tenant: String,
    status: String,
): List<build.crab.prolly.EntryRecord> {
    val start = statusIndexPrefix(tenant, status)
    return engine.range(index, start, build.crab.prolly.prefixEnd(start))
}

private fun bytes(value: String): ByteArray = value.encodeToByteArray()

private fun upsert(key: String, value: ByteArray): MutationRecord =
    MutationRecord(MutationKind.UPSERT, bytes(key), value)

private fun deleteMutation(key: String): MutationRecord =
    MutationRecord(MutationKind.DELETE, bytes(key), null)

private fun ByteArray.toHex(): String =
    joinToString(separator = "") { byte -> "%02x".format(byte.toInt() and 0xff) }

private fun requireBytes(expected: ByteArray, actual: ByteArray, label: String) {
    require(expected.contentEquals(actual)) { "$label mismatch" }
}
