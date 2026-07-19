package build.crab.prolly;

/** Stable publication-origin codes shared with the Rust engine and other bindings. */
public final class PublicationOrigins {
    public static final int GENERAL = 0;
    public static final int POINT_UPSERT = 1;
    public static final int POINT_DELETE = 2;
    public static final int BATCH_MUTATION = 3;
    public static final int TREE_BUILD = 4;
    public static final int MERGE = 5;
    public static final int RANGE_DELETE = 6;
    public static final int REPLICATION = 7;
    public static final int MAINTENANCE = 8;

    private PublicationOrigins() {
    }

    /** Maps future or otherwise unknown unsigned origin codes to the safe general path. */
    public static int normalizePublicationOriginCode(int code) {
        return Integer.compareUnsigned(code, MAINTENANCE) <= 0 ? code : GENERAL;
    }
}
