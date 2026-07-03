package build.crab.prolly.examples;

import build.crab.prolly.MutationRecord;
import build.crab.prolly.Prolly;
import build.crab.prolly.TreeRecord;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class VectorSidecar {
    private VectorSidecar() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        vectorSidecar();
    }

    private static void vectorSidecar() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            Map<String, double[]> sidecar = new LinkedHashMap<>();
            sidecar.put("vec-1", new double[] {0.9, 0.1});
            sidecar.put("vec-2", new double[] {0.8, 0.2});
            sidecar.put("vec-stale", new double[] {1.0, 0.0});
            TreeRecord tree = prolly.batch(prolly.create(), List.of(
                    upsertText("vector-sidecar/corpus/docs/chunk/doc-1/0001", "vec-1|doc-1|parser-v1"),
                    upsertText("vector-sidecar/corpus/docs/chunk/doc-2/0001", "vec-2|doc-2|parser-v1")));
            List<String> allowed = prolly.range(tree, bytes("vector-sidecar/corpus/docs/chunk/"), Optional.of(bytes("vector-sidecar/corpus/docs/chunk0")))
                    .stream()
                    .map(entry -> new String(entry.value(), StandardCharsets.UTF_8).split("\\|", 2)[0])
                    .toList();
            List<String> hits = sidecar.keySet().stream().sorted().filter(allowed::contains).toList();
            require(hits.equals(List.of("vec-1", "vec-2")), "unexpected sidecar hits");

            System.out.printf("vector_sidecar: filtered sidecar hits to %d snapshot vectors%n", hits.size());
        }
    }

    private static MutationRecord upsertText(String key, String value) {
        return Prolly.upsert(bytes(key), bytes(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
