package build.crab.prolly.examples

import build.crab.prolly.LargeValueConfigRecord
import build.crab.prolly.ProllyBlobStore
import build.crab.prolly.ProllyEngine
import build.crab.prolly.ProllyNative
import build.crab.prolly.ValueRefKind
import build.crab.prolly.defaultConfig

fun main() {
    ProllyNative.useLocalDebugLibrary()
    fileBlobStore()
}

private fun fileBlobStore() {
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

private fun bytes(value: String): ByteArray = value.encodeToByteArray()

private fun requireBytes(expected: ByteArray, actual: ByteArray, label: String) {
    require(expected.contentEquals(actual)) { "$label mismatch" }
}
