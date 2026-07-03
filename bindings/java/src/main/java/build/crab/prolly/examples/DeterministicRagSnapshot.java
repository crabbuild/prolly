package build.crab.prolly.examples;

import build.crab.prolly.MutationRecord;
import build.crab.prolly.Prolly;
import build.crab.prolly.TreeRecord;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

public final class DeterministicRagSnapshot {
    private DeterministicRagSnapshot() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        deterministicRagSnapshot();
    }

    private static void deterministicRagSnapshot() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            byte[] indexRoot = bytes("rag/corpus/docs/root/index/current");
            TreeRecord indexV1 = prolly.batch(prolly.create(), List.of(
                    upsertText("rag/corpus/docs/chunk/doc-1/0001", "vector:v1|CrabDB stores deterministic roots"),
                    upsertText("rag/corpus/docs/chunk/doc-2/0001", "vector:v2|Prolly trees diff by key")));
            prolly.publishNamedRoot(indexRoot, indexV1);
            TreeRecord answers = prolly.put(
                    prolly.create(),
                    bytes("rag/answer/q1"),
                    bytes("query:q1|snapshot:" + HexFormat.of().formatHex(indexV1.getRoot()) + "|citation:doc-1/0001"));
            prolly.publishNamedRoot(bytes("rag/corpus/docs/root/answers"), answers);

            TreeRecord indexV2 = prolly.put(indexV1, bytes("rag/corpus/docs/chunk/doc-3/0001"), bytes("vector:v3|New content"));
            prolly.publishNamedRoot(indexRoot, indexV2);

            int replayRows = prolly.range(indexV1, bytes("rag/corpus/docs/chunk/"), Optional.of(bytes("rag/corpus/docs/chunk0"))).size();
            int currentRows = prolly.range(prolly.loadNamedRoot(indexRoot).orElseThrow(), bytes("rag/corpus/docs/chunk/"), Optional.of(bytes("rag/corpus/docs/chunk0"))).size();
            require(replayRows == 2 && currentRows == 3, "RAG snapshot validation failed");

            System.out.println("deterministic_rag_snapshot: replay kept original index root");
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
