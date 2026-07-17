package build.crab.prolly.javaapi;

public enum IndexedUpdateKind {
    APPLIED,
    UNCHANGED,
    CONFLICT;

    static IndexedUpdateKind fromWire(String value) {
        return valueOf(value.toUpperCase(java.util.Locale.ROOT));
    }
}
