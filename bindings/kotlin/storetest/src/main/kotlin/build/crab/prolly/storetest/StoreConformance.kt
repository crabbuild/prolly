package build.crab.prolly.storetest

import build.crab.prolly.remote.NodeEntry
import build.crab.prolly.remote.NodeMutation
import build.crab.prolly.remote.OptionalBytes
import build.crab.prolly.remote.RemoteStore
import build.crab.prolly.remote.RootCondition
import build.crab.prolly.remote.RootWrite
import build.crab.prolly.remote.validateStoreDescriptor
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import java.util.UUID

object StoreConformance {
    private val objectMapper = ObjectMapper()

    suspend fun run(factory: suspend () -> RemoteStore) {
        val store = factory()
        val descriptor = validateStoreDescriptor(store.descriptor())
        check(descriptor.protocolMajor == 1u)
        val prefix = UUID.randomUUID().toString().take(8)

        val cases = loadCases()
        check(cases.path("protocol_major").asInt() == descriptor.protocolMajor.toInt())
        cases.path("cases").forEach { fixture ->
            val name = fixture.path("name").asText()
            val cid = "$prefix:$name".bytes()
            val present = fixture.path("present").asBoolean()
            val expected = fixture.path("hex").asText()
            if (present) store.putNode(cid, expected.hexBytes()) else store.deleteNode(cid)
            assertOptional(store.getNode(cid), present, expected)
        }

        val duplicate = "$prefix:duplicate".bytes()
        val missing = "$prefix:missing".bytes()
        store.putNode(duplicate, "value".bytes())
        val ordered = store.batchGetNodesOrdered(listOf(duplicate, missing, duplicate))
        check(ordered.size == 3) { "ordered batch changed result length" }
        assertOptional(ordered[0], true, "value".bytes().hex())
        assertOptional(ordered[1], false, "")
        assertOptional(ordered[2], true, "value".bytes().hex())

        val ownedCid = "$prefix:owned".bytes()
        val ownedValue = "owned-value".bytes()
        store.batchNodes(listOf(NodeMutation.Upsert(ownedCid, ownedValue)))
        ownedCid.fill(0)
        ownedValue.fill(0)
        assertOptional(store.getNode("$prefix:owned".bytes()), true, "owned-value".bytes().hex())

        if (descriptor.capabilities.nodeScan) {
            val cids = store.listNodeCids()
            check(cids.zipWithNext().all { (left, right) -> left.compareUnsigned(right) <= 0 }) {
                "node scan is not unsigned-byte sorted"
            }
        }

        if (descriptor.capabilities.hints) {
            val namespace = "$prefix:namespace".bytes()
            val key = "hint-key".bytes()
            store.putHint(namespace, key, "hint-value".bytes())
            assertOptional(store.getHint(namespace, key), true, "hint-value".bytes().hex())
            store.batchPutNodesWithHint(
                listOf(NodeEntry("$prefix:hint-node".bytes(), "hint-node-value".bytes())),
                namespace,
                "batch-key".bytes(),
                "batch-value".bytes(),
            )
            assertOptional(
                store.getNode("$prefix:hint-node".bytes()),
                true,
                "hint-node-value".bytes().hex(),
            )
        }

        val casRoot = "$prefix:cas".bytes()
        if (descriptor.capabilities.rootCompareAndSwap) {
            check(
                store.compareAndSwapRootManifest(
                    casRoot,
                    OptionalBytes.missing(),
                    OptionalBytes.present("one".bytes()),
                ).applied,
            )
            val conflict = store.compareAndSwapRootManifest(
                casRoot,
                OptionalBytes.missing(),
                OptionalBytes.present("two".bytes()),
            )
            check(!conflict.applied)
            assertOptional(conflict.current, true, "one".bytes().hex())
            check(
                store.compareAndSwapRootManifest(
                    casRoot,
                    OptionalBytes.present("one".bytes()),
                    OptionalBytes.missing(),
                ).applied,
            )
        }

        if (descriptor.capabilities.transactions) {
            verifyTransactions(store, prefix)
        }

        val failures = loadFailures()
        check(failures.path("protocol_major").asInt() == 1)
        val absentWithValue = failures.path("cases").first { it.path("name").asText() == "absent-with-value" }
        check(absentWithValue.path("present").asBoolean().not())
        check(runCatching { OptionalBytes.of(false, absentWithValue.path("hex").asText().hexBytes()) }.isFailure)
    }

    private suspend fun verifyTransactions(store: RemoteStore, prefix: String) {
        val root = "$prefix:transaction".bytes()
        val node = "$prefix:tx-node".bytes()
        val conflict = store.commitTransaction(
            listOf(NodeMutation.Upsert(node, "must-not-write".bytes())),
            listOf(RootCondition(root, OptionalBytes.present("wrong".bytes()))),
            listOf(RootWrite.Put(root, "must-not-publish".bytes())),
        )
        check(!conflict.applied)
        assertOptional(store.getNode(node), false, "")
        assertOptional(store.getRootManifest(root), false, "")

        val applied = store.commitTransaction(
            listOf(NodeMutation.Upsert(node, "written".bytes())),
            listOf(RootCondition(root, OptionalBytes.missing())),
            listOf(RootWrite.Put(root, "published".bytes())),
        )
        check(applied.applied)
        assertOptional(store.getNode(node), true, "written".bytes().hex())
        assertOptional(store.getRootManifest(root), true, "published".bytes().hex())
        store.commitTransaction(emptyList(), emptyList(), listOf(RootWrite.Delete(root)))
    }

    private fun loadCases(): JsonNode = load("store-protocol-v1/cases.json")

    private fun loadFailures(): JsonNode = load("store-protocol-v1/failure-cases.json")

    private fun load(resource: String): JsonNode {
        val stream = checkNotNull(StoreConformance::class.java.classLoader.getResourceAsStream(resource)) {
            "missing conformance resource $resource"
        }
        return stream.use(objectMapper::readTree)
    }

    private fun assertOptional(value: OptionalBytes, present: Boolean, expectedHex: String) {
        check(value.present == present) { "optional presence mismatch" }
        check(value.value.hex() == expectedHex) { "optional bytes mismatch" }
    }
}

private fun String.bytes(): ByteArray = encodeToByteArray()

private fun String.hexBytes(): ByteArray {
    require(length % 2 == 0)
    return ByteArray(length / 2) { index -> substring(index * 2, index * 2 + 2).toInt(16).toByte() }
}

private fun ByteArray.hex(): String = joinToString("") { "%02x".format(it.toInt() and 0xff) }

private fun ByteArray.compareUnsigned(other: ByteArray): Int {
    val size = minOf(size, other.size)
    for (index in 0 until size) {
        val compared = this[index].toUByte().compareTo(other[index].toUByte())
        if (compared != 0) return compared
    }
    return this.size.compareTo(other.size)
}
