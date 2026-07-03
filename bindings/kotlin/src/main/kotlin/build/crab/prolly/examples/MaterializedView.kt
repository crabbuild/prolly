package build.crab.prolly.examples

import build.crab.prolly.MutationKind
import build.crab.prolly.MutationRecord
import build.crab.prolly.ProllyEngine
import build.crab.prolly.ProllyNative
import build.crab.prolly.TreeRecord
import build.crab.prolly.defaultConfig

fun main() {
    ProllyNative.useLocalDebugLibrary()
    materializedView()
}

private fun materializedView() {
    ProllyEngine.memory(defaultConfig()).use { engine ->
        val o1 = Order("acme", "o1", "paid", 1200)
        val o2 = Order("acme", "o2", "open", 500)
        val sourceV1 =
            engine.batch(
                engine.create(),
                listOf(
                    MutationRecord(MutationKind.UPSERT, orderKey(o1), encodeOrder(o1)),
                    MutationRecord(MutationKind.UPSERT, orderKey(o2), encodeOrder(o2)),
                ),
            )
        val paidO2 = Order("acme", "o2", "paid", 500)
        val sourceV2 = engine.put(sourceV1, orderKey(paidO2), encodeOrder(paidO2))
        val viewV2 = buildRevenueView(engine, sourceV2)

        requireBytes(bytes("1700"), requireNotNull(engine.get(viewV2, viewKey("acme", "paid"))), "paid revenue")
        require(engine.get(viewV2, viewKey("acme", "open")) == null) { "open revenue should be absent" }

        println("materialized_view: folded ${engine.diff(sourceV1, sourceV2).size} source diff")
    }
}

private data class Order(val tenant: String, val id: String, val status: String, val cents: Int)

private fun orderKey(order: Order): ByteArray =
    bytes("orders/source/tenant/${order.tenant}/order/${order.id}")

private fun encodeOrder(order: Order): ByteArray =
    bytes("${order.tenant}|${order.id}|${order.status}|${order.cents}")

private fun decodeOrder(value: ByteArray): Order {
    val parts = value.decodeToString().split("|", limit = 4)
    return Order(parts[0], parts[1], parts[2], parts[3].toInt())
}

private fun viewKey(tenant: String, status: String): ByteArray =
    bytes("orders/view/by-status/tenant/$tenant/status/$status")

private fun buildRevenueView(engine: ProllyEngine, source: TreeRecord): TreeRecord {
    val totals = linkedMapOf<Pair<String, String>, Int>()
    for (entry in engine.range(source, bytes("orders/source/"), bytes("orders/source0"))) {
        val order = decodeOrder(entry.value)
        val key = order.tenant to order.status
        totals[key] = (totals[key] ?: 0) + order.cents
    }
    val mutations =
        totals.entries
            .sortedBy { (key, _) -> "${key.first}|${key.second}" }
            .map { (key, cents) -> MutationRecord(MutationKind.UPSERT, viewKey(key.first, key.second), cents.toString().encodeToByteArray()) }
    return engine.batch(engine.create(), mutations)
}

private fun bytes(value: String): ByteArray = value.encodeToByteArray()

private fun requireBytes(expected: ByteArray, actual: ByteArray, label: String) {
    require(expected.contentEquals(actual)) { "$label mismatch" }
}
