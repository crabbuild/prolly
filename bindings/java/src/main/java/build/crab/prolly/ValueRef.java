package build.crab.prolly;

import java.util.Optional;

public final class ValueRef {
    public enum Kind {
        INLINE,
        BLOB
    }

    private final Kind kind;
    private final byte[] value;
    private final BlobRef blob;

    ValueRef(ValueRefRecord record) {
        this.kind = record.getKind() == ValueRefKind.INLINE ? Kind.INLINE : Kind.BLOB;
        byte[] recordValue = record.getValue();
        this.value = recordValue == null ? null : recordValue.clone();
        BlobRefRecord recordBlob = record.getBlob();
        this.blob = recordBlob == null ? null : new BlobRef(recordBlob);
    }

    public Kind kind() {
        return kind;
    }

    public Optional<byte[]> value() {
        return value == null ? Optional.empty() : Optional.of(value.clone());
    }

    public Optional<BlobRef> blob() {
        return Optional.ofNullable(blob);
    }
}
