package build.crab.prolly.javaapi;

import build.crab.prolly.IndexProjectionRecord;

public enum IndexProjection {
    KEYS_ONLY(IndexProjectionRecord.KEYS_ONLY),
    INCLUDE(IndexProjectionRecord.INCLUDE),
    ALL(IndexProjectionRecord.ALL);

    final IndexProjectionRecord nativeValue;

    IndexProjection(IndexProjectionRecord nativeValue) {
        this.nativeValue = nativeValue;
    }
}
