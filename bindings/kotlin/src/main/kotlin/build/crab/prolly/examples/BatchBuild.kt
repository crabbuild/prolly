package build.crab.prolly.examples

import build.crab.prolly.EntryRecord
import build.crab.prolly.ProllyEngine
import build.crab.prolly.ProllyNative
import build.crab.prolly.defaultConfig

fun main() {
    ProllyNative.useLocalDebugLibrary()
    batchBuild()
}

private fun batchBuild() {
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

private fun bytes(value: String): ByteArray = value.encodeToByteArray()

private fun requireBytes(expected: ByteArray, actual: ByteArray, label: String) {
    require(expected.contentEquals(actual)) { "$label mismatch" }
}
