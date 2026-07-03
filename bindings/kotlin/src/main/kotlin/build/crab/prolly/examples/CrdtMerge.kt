package build.crab.prolly.examples

import build.crab.prolly.CrdtDeletePolicyKind
import build.crab.prolly.ProllyEngine
import build.crab.prolly.ProllyNative
import build.crab.prolly.TimestampedValueRecord
import build.crab.prolly.crdtConfigLww
import build.crab.prolly.defaultConfig
import build.crab.prolly.multiValueSetMerge
import build.crab.prolly.timestampedValueFromBytes
import build.crab.prolly.timestampedValueToBytes

fun main() {
    ProllyNative.useLocalDebugLibrary()
    crdtMerge()
}

private fun crdtMerge() {
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

private fun bytes(value: String): ByteArray = value.encodeToByteArray()

private fun requireBytes(expected: ByteArray, actual: ByteArray, label: String) {
    require(expected.contentEquals(actual)) { "$label mismatch" }
}
