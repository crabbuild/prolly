package build.crab.prolly.examples

import build.crab.prolly.ProllyEngine
import build.crab.prolly.ProllyNative
import build.crab.prolly.defaultConfig

fun main() {
    ProllyNative.useLocalDebugLibrary()
    resolver()
}

private fun resolver() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        val base = engine.put(engine.create(), bytes("settings/theme"), bytes("light"))
        val leftDelete = engine.delete(base, bytes("settings/theme"))
        val rightUpdate = engine.put(base, bytes("settings/theme"), bytes("dark"))

        val updateWins = engine.merge(base, leftDelete, rightUpdate, "update_wins")
        val deleteWins = engine.merge(base, leftDelete, rightUpdate, "delete_wins")

        requireBytes(bytes("dark"), requireNotNull(engine.get(updateWins, bytes("settings/theme"))), "update-wins setting")
        require(engine.get(deleteWins, bytes("settings/theme")) == null) { "delete-wins should remove setting" }

        println("resolver: demonstrated update-wins and delete-wins policies")
    }
}

private fun bytes(value: String): ByteArray = value.encodeToByteArray()

private fun requireBytes(expected: ByteArray, actual: ByteArray, label: String) {
    require(expected.contentEquals(actual)) { "$label mismatch" }
}
