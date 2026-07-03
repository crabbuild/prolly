package build.crab.prolly;

import java.util.Arrays;

public final class Entry {
    private final byte[] key;
    private final byte[] value;

    public Entry(byte[] key, byte[] value) {
        this.key = key.clone();
        this.value = value.clone();
    }

    public byte[] key() {
        return key.clone();
    }

    public byte[] value() {
        return value.clone();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Entry entry)) {
            return false;
        }
        return Arrays.equals(key, entry.key) && Arrays.equals(value, entry.value);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(key) + Arrays.hashCode(value);
    }
}
