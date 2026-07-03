package build.crab.prolly.examples

import build.crab.prolly.ProllyEngine
import build.crab.prolly.ProllyNative
import build.crab.prolly.defaultConfig

fun main() {
    ProllyNative.useLocalDebugLibrary()
    basicMap()
}

private fun basicMap() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        var tree = engine.create()
        tree = engine.put(tree, bytes("user:001"), bytes("Ada"))
        tree = engine.put(tree, bytes("user:002"), bytes("Grace"))
        tree = engine.put(tree, bytes("user:003"), bytes("Linus"))

        requireBytes(bytes("Ada"), requireNotNull(engine.get(tree, bytes("user:001"))), "user:001")

        tree = engine.delete(tree, bytes("user:003"))
        require(engine.get(tree, bytes("user:003")) == null) { "user:003 should be deleted" }

        val users = engine.range(tree, bytes("user:"), bytes("user;"))
        require(users.size == 2) { "expected two users" }
        requireBytes(bytes("user:001"), users[0].key, "first key")
        requireBytes(bytes("Ada"), users[0].value, "first value")
        requireBytes(bytes("user:002"), users[1].key, "second key")
        requireBytes(bytes("Grace"), users[1].value, "second value")

        println("basic_map: ${users.size} users in range")
    }
}

private fun bytes(value: String): ByteArray = value.encodeToByteArray()

private fun requireBytes(expected: ByteArray, actual: ByteArray, label: String) {
    require(expected.contentEquals(actual)) { "$label mismatch" }
}
