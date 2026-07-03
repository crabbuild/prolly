package build.crab.prolly.examples

import build.crab.prolly.MutationKind
import build.crab.prolly.MutationRecord
import build.crab.prolly.ProllyEngine
import build.crab.prolly.ProllyNative
import build.crab.prolly.defaultConfig

fun main() {
    ProllyNative.useLocalDebugLibrary()
    vectorSidecar()
}

private fun vectorSidecar() {
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

private fun bytes(value: String): ByteArray = value.encodeToByteArray()

private fun upsert(key: String, value: ByteArray): MutationRecord =
    MutationRecord(MutationKind.UPSERT, bytes(key), value)
