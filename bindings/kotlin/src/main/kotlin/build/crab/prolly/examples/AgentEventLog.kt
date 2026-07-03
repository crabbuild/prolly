package build.crab.prolly.examples

import build.crab.prolly.MutationKind
import build.crab.prolly.MutationRecord
import build.crab.prolly.ProllyEngine
import build.crab.prolly.ProllyNative
import build.crab.prolly.defaultConfig

fun main() {
    ProllyNative.useLocalDebugLibrary()
    agentEventLog()
}

private fun agentEventLog() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        val root = bytes("agent-log/run-7/root/events/current")
        val tree =
            engine.batch(
                engine.create(),
                listOf(
                    upsert("agent-log/run-7/event/1783036805000/0001", bytes("user|Summarize the plan")),
                    upsert("agent-log/run-7/event/1783036805000/0002", bytes("tool-call|search-docs")),
                    upsert("agent-log/run-7/event/1783036806000/0003", bytes("assistant|Plan ready")),
                ),
            )
        engine.publishNamedRoot(root, tree)

        val page = engine.rangePage(requireNotNull(engine.loadNamedRoot(root)), null, null, 2UL)
        require(page.entries.size == 2) { "expected first event page" }
        require(page.nextCursor != null) { "expected next cursor" }

        println("agent_event_log: first page has ${page.entries.size} events")
    }
}

private fun bytes(value: String): ByteArray = value.encodeToByteArray()

private fun upsert(key: String, value: ByteArray): MutationRecord =
    MutationRecord(MutationKind.UPSERT, bytes(key), value)
