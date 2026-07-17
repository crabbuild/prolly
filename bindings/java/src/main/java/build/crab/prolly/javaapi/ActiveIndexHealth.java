package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaActiveIndexHealth;

public record ActiveIndexHealth(
        byte[] name,
        long generation,
        byte[] fingerprint,
        IndexProjection projection,
        byte[] indexMapId,
        byte[] indexVersion) {
    static ActiveIndexHealth fromNative(JavaActiveIndexHealth value) {
        return new ActiveIndexHealth(
                value.getName().clone(), value.getGeneration(), value.getFingerprint().clone(),
                IndexProjection.valueOf(value.getProjection().toUpperCase(java.util.Locale.ROOT)),
                value.getIndexMapId().clone(), value.getIndexVersion().clone());
    }
}
