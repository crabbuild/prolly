package build.crab.prolly.examples

import build.crab.prolly.MutationKind
import build.crab.prolly.MutationRecord
import build.crab.prolly.ProllyEngine
import build.crab.prolly.ProllyNative
import build.crab.prolly.defaultConfig

fun main() {
    ProllyNative.useLocalDebugLibrary()
    deterministicRagSnapshot()
}

private fun deterministicRagSnapshot() {
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

private fun bytes(value: String): ByteArray = value.encodeToByteArray()

private fun upsert(key: String, value: ByteArray): MutationRecord =
    MutationRecord(MutationKind.UPSERT, bytes(key), value)

private fun ByteArray.toHex(): String =
    joinToString(separator = "") { byte -> "%02x".format(byte.toInt() and 0xff) }
