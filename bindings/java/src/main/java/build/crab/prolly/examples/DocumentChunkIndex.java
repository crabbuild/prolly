package build.crab.prolly.examples;

import build.crab.prolly.BlobStore;
import build.crab.prolly.Prolly;
import build.crab.prolly.TreeRecord;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public final class DocumentChunkIndex {
    private DocumentChunkIndex() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        documentChunkIndex();
    }

    private static void documentChunkIndex() throws Exception {
        try (Prolly prolly = Prolly.memory(); BlobStore blobStore = BlobStore.memory()) {
            byte[] textKey = bytes("doc-index/corpus/text/parser-v1/doc-1/chunk-0001");
            byte[] metadataKey = bytes("doc-index/corpus/parser/parser-v1/document/doc-1/chunk/000000");
            TreeRecord tree = prolly.putLargeValue(
                    blobStore,
                    prolly.create(),
                    textKey,
                    bytes("CrabDB stores large chunk text outside prolly leaves.".repeat(8)),
                    Prolly.largeValueConfig(32));
            tree = prolly.put(tree, metadataKey, bytes("doc-1|chunk-0001|0|384|vector-0001"));

            require(prolly.range(tree, bytes("doc-index/corpus/parser/"), Optional.of(bytes("doc-index/corpus/parser0"))).size() == 1, "metadata missing");
            require(new String(prolly.getLargeValue(blobStore, tree, textKey).orElseThrow(), StandardCharsets.UTF_8).startsWith("CrabDB stores"), "chunk text missing");

            System.out.println("document_chunk_index: metadata and blob-backed chunk text are linked");
        }
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
