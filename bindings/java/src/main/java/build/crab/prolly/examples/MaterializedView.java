package build.crab.prolly.examples;

import build.crab.prolly.Entry;
import build.crab.prolly.MutationRecord;
import build.crab.prolly.Prolly;
import build.crab.prolly.TreeRecord;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class MaterializedView {
    private MaterializedView() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        materializedView();
    }

    private static void materializedView() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            Order o1 = new Order("acme", "o1", "paid", 1200);
            Order o2 = new Order("acme", "o2", "open", 500);
            TreeRecord sourceV1 = prolly.batch(prolly.create(), List.of(
                    Prolly.upsert(orderKey(o1), encodeOrder(o1)),
                    Prolly.upsert(orderKey(o2), encodeOrder(o2))));
            Order paidO2 = new Order("acme", "o2", "paid", 500);
            TreeRecord sourceV2 = prolly.put(sourceV1, orderKey(paidO2), encodeOrder(paidO2));
            TreeRecord viewV2 = buildRevenueView(prolly, sourceV2);

            requireBytes(bytes("1700"), prolly.get(viewV2, viewKey("acme", "paid")).orElseThrow(), "paid revenue");
            require(prolly.get(viewV2, viewKey("acme", "open")).isEmpty(), "open revenue should be absent");

            System.out.printf("materialized_view: folded %d source diff%n", prolly.diff(sourceV1, sourceV2).size());
        }
    }

    private record Order(String tenant, String id, String status, int cents) {
    }

    private static byte[] orderKey(Order order) {
        return bytes("orders/source/tenant/" + order.tenant() + "/order/" + order.id());
    }

    private static byte[] encodeOrder(Order order) {
        return bytes(order.tenant() + "|" + order.id() + "|" + order.status() + "|" + order.cents());
    }

    private static Order decodeOrder(byte[] value) {
        String[] parts = new String(value, StandardCharsets.UTF_8).split("\\|", 4);
        return new Order(parts[0], parts[1], parts[2], Integer.parseInt(parts[3]));
    }

    private static byte[] viewKey(String tenant, String status) {
        return bytes("orders/view/by-status/tenant/" + tenant + "/status/" + status);
    }

    private static TreeRecord buildRevenueView(Prolly prolly, TreeRecord source) throws Exception {
        Map<String, Integer> totals = new LinkedHashMap<>();
        for (Entry entry : prolly.range(source, bytes("orders/source/"), Optional.of(bytes("orders/source0")))) {
            Order order = decodeOrder(entry.value());
            String key = order.tenant() + "|" + order.status();
            totals.put(key, totals.getOrDefault(key, 0) + order.cents());
        }
        List<MutationRecord> mutations = totals.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> {
                    String[] parts = entry.getKey().split("\\|", 2);
                    return Prolly.upsert(viewKey(parts[0], parts[1]), bytes(Integer.toString(entry.getValue())));
                })
                .toList();
        return prolly.batch(prolly.create(), mutations);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static void requireBytes(byte[] expected, byte[] actual, String label) {
        if (!Arrays.equals(expected, actual)) {
            throw new IllegalStateException(label + " mismatch");
        }
    }
}
