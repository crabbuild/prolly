package build.crab.prolly;

public final class LargeValueConfig {
    private final long inlineThreshold;

    public LargeValueConfig(long inlineThreshold) {
        this.inlineThreshold = inlineThreshold;
    }

    LargeValueConfig(LargeValueConfigRecord record) {
        this(ProllyJavaAdapters.largeValueConfigInlineThreshold(record));
    }

    LargeValueConfigRecord toRecord() {
        return ProllyJavaAdapters.largeValueConfig(inlineThreshold);
    }

    public long inlineThreshold() {
        return inlineThreshold;
    }
}
