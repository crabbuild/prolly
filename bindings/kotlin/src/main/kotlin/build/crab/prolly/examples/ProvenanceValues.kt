package build.crab.prolly.examples

import build.crab.prolly.MutationKind
import build.crab.prolly.MutationRecord
import build.crab.prolly.ProllyEngine
import build.crab.prolly.ProllyNative
import build.crab.prolly.cidFromBytes
import build.crab.prolly.defaultConfig

fun main() {
    ProllyNative.useLocalDebugLibrary()
    provenanceValues()
}

private fun provenanceValues() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        val source = bytes("CrabDB language bindings design")
        val sourceCid = cidFromBytes(source).toHex()
        val chunkCid = cidFromBytes(source.copyOfRange(0, 16)).toHex()
        val tree =
            engine.batch(
                engine.create(),
                listOf(
                    upsert("provenance/chunk/file-1/chunk-1", bytes("source=$sourceCid|chunk=$chunkCid|parser=v1")),
                    upsert("provenance/claim/file-1/claim-1", bytes("CrabDB uses Rust-backed bindings|chunk=file-1/chunk-1")),
                ),
            )

        val claims = engine.range(tree, bytes("provenance/claim/file-1/"), bytes("provenance/claim/file-10"))
        require(claims.size == 1) { "expected one claim" }
        require(claims.first().value.decodeToString().contains("Rust-backed")) { "missing claim text" }

        println("provenance_values: claim links back to source and chunk CIDs")
    }
}

private fun bytes(value: String): ByteArray = value.encodeToByteArray()

private fun upsert(key: String, value: ByteArray): MutationRecord =
    MutationRecord(MutationKind.UPSERT, bytes(key), value)

private fun ByteArray.toHex(): String =
    joinToString(separator = "") { byte -> "%02x".format(byte.toInt() and 0xff) }
