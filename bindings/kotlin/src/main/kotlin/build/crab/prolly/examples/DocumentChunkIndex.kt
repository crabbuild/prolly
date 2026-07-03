package build.crab.prolly.examples

import build.crab.prolly.LargeValueConfigRecord
import build.crab.prolly.ProllyBlobStore
import build.crab.prolly.ProllyEngine
import build.crab.prolly.ProllyNative
import build.crab.prolly.defaultConfig

fun main() {
    ProllyNative.useLocalDebugLibrary()
    documentChunkIndex()
}

private fun documentChunkIndex() {
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

private fun bytes(value: String): ByteArray = value.encodeToByteArray()
