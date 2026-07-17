package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaFullRebuildReason;

public record FullRebuildReason(Kind kind, long actual, long maximum) {
    public enum Kind { DELTA_RECORDS, SHADOW_RECORDS, DELTA_RATIO, SHADOW_RATIO }
    static FullRebuildReason fromNative(JavaFullRebuildReason value) {
        return new FullRebuildReason(Kind.valueOf(value.getKind()), value.getActual(), value.getMaximum());
    }
}
