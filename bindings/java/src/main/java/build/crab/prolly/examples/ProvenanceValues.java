package build.crab.prolly.examples;

import build.crab.prolly.Entry;
import build.crab.prolly.MutationRecord;
import build.crab.prolly.Prolly;
import build.crab.prolly.TreeRecord;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

public final class ProvenanceValues {
    private ProvenanceValues() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        provenanceValues();
    }

    private static void provenanceValues() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            byte[] source = bytes("CrabDB language bindings design");
            String sourceCid = HexFormat.of().formatHex(Prolly.cidFromBytes(source));
            String chunkCid = HexFormat.of().formatHex(Prolly.cidFromBytes(Arrays.copyOfRange(source, 0, 16)));
            TreeRecord tree = prolly.batch(prolly.create(), List.of(
                    upsertText("provenance/chunk/file-1/chunk-1", "source=" + sourceCid + "|chunk=" + chunkCid + "|parser=v1"),
                    upsertText("provenance/claim/file-1/claim-1", "CrabDB uses Rust-backed bindings|chunk=file-1/chunk-1")));
            List<Entry> claims = prolly.range(tree, bytes("provenance/claim/file-1/"), Optional.of(bytes("provenance/claim/file-10")));
            require(claims.size() == 1, "expected one claim");
            require(new String(claims.get(0).value(), StandardCharsets.UTF_8).contains("Rust-backed"), "missing claim text");

            System.out.println("provenance_values: claim links back to source and chunk CIDs");
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
