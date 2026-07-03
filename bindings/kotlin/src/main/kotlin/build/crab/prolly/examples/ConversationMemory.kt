package build.crab.prolly.examples

import build.crab.prolly.ProllyEngine
import build.crab.prolly.ProllyNative
import build.crab.prolly.defaultConfig

fun main() {
    ProllyNative.useLocalDebugLibrary()
    conversationMemory()
}

private fun conversationMemory() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        val main = bytes("conversation/c42/root/main")
        val attemptName = bytes("conversation/c42/attempt/extractor/a1")
        val base = engine.put(engine.create(), bytes("conversation/c42/memory/m001"), bytes("user|likes terse summaries|0.91"))
        engine.publishNamedRoot(main, base)
        val attempt = engine.put(base, bytes("conversation/c42/memory/m002"), bytes("user|uses Kotlin|0.87"))
        engine.publishNamedRoot(attemptName, attempt)
        val canonical = engine.put(base, bytes("conversation/c42/memory/m003"), bytes("user|prefers local-first apps|0.82"))
        engine.publishNamedRoot(main, canonical)

        val merged = engine.merge(
            base,
            requireNotNull(engine.loadNamedRoot(main)),
            requireNotNull(engine.loadNamedRoot(attemptName)),
            "prefer_right",
        )
        val update = engine.compareAndSwapNamedRoot(main, canonical, merged)
        val rows = engine.range(merged, bytes("conversation/c42/memory/"), bytes("conversation/c42/memory0"))

        require(update.applied) { "main root CAS failed" }
        require(rows.size == 3) { "expected three memories" }

        println("conversation_memory: accepted extractor attempt into canonical memory")
    }
}

private fun bytes(value: String): ByteArray = value.encodeToByteArray()
