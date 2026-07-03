package build.crab.prolly.examples

import build.crab.prolly.MutationKind
import build.crab.prolly.MutationRecord
import build.crab.prolly.ProllyEngine
import build.crab.prolly.ProllyNative
import build.crab.prolly.defaultConfig
import java.nio.file.Files

fun main() {
    ProllyNative.useLocalDebugLibrary()
    durableSqlite()
}

private fun durableSqlite() {
    val dir = Files.createTempDirectory("prolly-kotlin-")
    try {
        ProllyEngine.sqlite(dir.resolve("app.prolly.sqlite").toString(), defaultConfig()).use { engine ->
            val tree = engine.batch(
                engine.create(),
                listOf(upsert("user/1", bytes("Ada")), upsert("user/2", bytes("Grace"))),
            )
            engine.publishNamedRoot(bytes("users/main"), tree)
            val loaded = requireNotNull(engine.loadNamedRoot(bytes("users/main")))
            require(loaded.root?.contentEquals(tree.root) == true) { "loaded SQLite root mismatch" }
            requireBytes(bytes("Ada"), requireNotNull(engine.get(loaded, bytes("user/1"))), "sqlite user")
        }
    } finally {
        dir.toFile().deleteRecursively()
    }
    println("durable_sqlite: named root survived through SQLite store API")
}

private fun bytes(value: String): ByteArray = value.encodeToByteArray()

private fun upsert(key: String, value: ByteArray): MutationRecord =
    MutationRecord(MutationKind.UPSERT, bytes(key), value)

private fun requireBytes(expected: ByteArray, actual: ByteArray, label: String) {
    require(expected.contentEquals(actual)) { "$label mismatch" }
}
