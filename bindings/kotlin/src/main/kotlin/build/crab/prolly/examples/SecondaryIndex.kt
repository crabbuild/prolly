package build.crab.prolly.examples

import build.crab.prolly.DiffKind
import build.crab.prolly.EntryRecord
import build.crab.prolly.ProllyEngine
import build.crab.prolly.ProllyNative
import build.crab.prolly.TreeRecord
import build.crab.prolly.defaultConfig

fun main() {
    ProllyNative.useLocalDebugLibrary()
    secondaryIndex()
}

private fun secondaryIndex() {
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
