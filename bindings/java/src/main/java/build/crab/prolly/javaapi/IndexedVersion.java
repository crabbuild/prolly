package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaIndexedVersion;

public record IndexedVersion(byte[] sourceVersion, byte[] stateVersion, long indexCount) {
    static IndexedVersion fromNative(JavaIndexedVersion value) {
        return new IndexedVersion(
                value.getSourceVersion().clone(),
                value.getStateVersion().clone(),
                value.getIndexCount());
    }
}
