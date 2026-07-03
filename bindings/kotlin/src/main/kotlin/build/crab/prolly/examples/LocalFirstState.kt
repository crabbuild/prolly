package build.crab.prolly.examples

import build.crab.prolly.MutationKind
import build.crab.prolly.MutationRecord
import build.crab.prolly.ProllyEngine
import build.crab.prolly.ProllyNative
import build.crab.prolly.defaultConfig

fun main() {
    ProllyNative.useLocalDebugLibrary()
    localFirstState()
}

private fun localFirstState() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        val main = bytes("app/demo/root/main")
        val base =
            engine.batch(
                engine.create(),
                listOf(
                    upsert("entity/user/001", bytes("Ada")),
                    upsert("index/user/name/Ada/001", ByteArray(0)),
                ),
            )
        engine.publishNamedRoot(main, base)

        val device =
            engine.batch(
                base,
                listOf(
                    upsert("entity/task/900", bytes("offline draft")),
                    upsert("index/task/status/open/900", ByteArray(0)),
                ),
            )
        val canonical = engine.put(base, bytes("entity/user/002"), bytes("Grace"))
        engine.publishNamedRoot(main, canonical)

        val current = requireNotNull(engine.loadNamedRoot(main))
        val merged = engine.merge(base, current, device, "prefer_right")
        val update = engine.compareAndSwapNamedRoot(main, current, merged)

        require(update.applied) { "main root CAS failed" }
        requireBytes(bytes("Grace"), requireNotNull(engine.get(merged, bytes("entity/user/002"))), "canonical user")
        requireBytes(bytes("offline draft"), requireNotNull(engine.get(merged, bytes("entity/task/900"))), "device task")

        println("local_first_state: merged offline branch into main")
    }
}

private fun bytes(value: String): ByteArray = value.encodeToByteArray()

private fun upsert(key: String, value: ByteArray): MutationRecord =
    MutationRecord(MutationKind.UPSERT, bytes(key), value)

private fun requireBytes(expected: ByteArray, actual: ByteArray, label: String) {
    require(expected.contentEquals(actual)) { "$label mismatch" }
}
