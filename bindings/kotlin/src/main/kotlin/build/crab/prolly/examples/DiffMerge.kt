package build.crab.prolly.examples

import build.crab.prolly.ProllyEngine
import build.crab.prolly.ProllyNative
import build.crab.prolly.defaultConfig

fun main() {
    ProllyNative.useLocalDebugLibrary()
    diffMerge()
}

private fun diffMerge() {
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

private fun bytes(value: String): ByteArray = value.encodeToByteArray()

private fun requireBytes(expected: ByteArray, actual: ByteArray, label: String) {
    require(expected.contentEquals(actual)) { "$label mismatch" }
}
