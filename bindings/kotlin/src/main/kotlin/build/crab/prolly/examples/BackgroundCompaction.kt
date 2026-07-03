package build.crab.prolly.examples

import build.crab.prolly.MutationKind
import build.crab.prolly.MutationRecord
import build.crab.prolly.ProllyEngine
import build.crab.prolly.ProllyNative
import build.crab.prolly.defaultConfig

fun main() {
    ProllyNative.useLocalDebugLibrary()
    backgroundCompaction()
}

private fun backgroundCompaction() {
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

private fun bytes(value: String): ByteArray = value.encodeToByteArray()

private fun upsert(key: String, value: ByteArray): MutationRecord =
    MutationRecord(MutationKind.UPSERT, bytes(key), value)

private fun deleteMutation(key: String): MutationRecord =
    MutationRecord(MutationKind.DELETE, bytes(key), null)
