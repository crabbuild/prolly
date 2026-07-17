package build.crab.prolly.javaapi;

import java.util.ArrayList;
import java.util.List;

public record ProximityRecord(byte[] key, float[] vector, byte[] value) {
    public ProximityRecord {
        key = key.clone(); vector = vector.clone(); value = value == null ? new byte[0] : value.clone();
    }
    build.crab.prolly.api.ProximityRecord toNative() {
        List<Float> values = new ArrayList<>(vector.length);
        for (float item : vector) values.add(item);
        return new build.crab.prolly.api.ProximityRecord(key.clone(), values, value.clone());
    }
}
