package build.crab.prolly.examples

import build.crab.prolly.LargeValueConfigRecord
import build.crab.prolly.ProllyBlobStore
import build.crab.prolly.ProllyEngine
import build.crab.prolly.ProllyNative
import build.crab.prolly.defaultConfig

fun main() {
    ProllyNative.useLocalDebugLibrary()
    filesystemSnapshot()
}

private fun filesystemSnapshot() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        ProllyBlobStore.memory().use { blobStore ->
            var tree = engine.create()
            for ((path, contents) in mapOf("README.md" to "# Demo\n", "src/lib.rs" to "pub fn answer() -> u8 { 42 }\n")) {
                tree = engine.putLargeValue(blobStore, tree, bytes("path/$path"), bytes(contents), LargeValueConfigRecord(4UL))
            }
            engine.publishNamedRoot(bytes("refs/heads/main"), tree)
            val loaded = requireNotNull(engine.loadNamedRoot(bytes("refs/heads/main")))
            requireBytes(bytes("# Demo\n"), requireNotNull(engine.getLargeValue(blobStore, loaded, bytes("path/README.md"))), "README.md")

            println("filesystem_snapshot: published branch with blob-backed file contents")
        }
    }
}

private fun bytes(value: String): ByteArray = value.encodeToByteArray()

private fun requireBytes(expected: ByteArray, actual: ByteArray, label: String) {
    require(expected.contentEquals(actual)) { "$label mismatch" }
}
