package build.crab.prolly;

import java.util.ArrayList;
import java.util.List;

public record SnapshotSelection(List<SnapshotRoot> snapshots, List<byte[]> missingIds) {
    public SnapshotSelection {
        snapshots = List.copyOf(snapshots);
        missingIds = cloneByteArrays(missingIds);
    }

    @Override
    public List<byte[]> missingIds() {
        return cloneByteArrays(missingIds);
    }

    private static List<byte[]> cloneByteArrays(List<byte[]> values) {
        List<byte[]> cloned = new ArrayList<>(values.size());
        for (byte[] value : values) {
            cloned.add(value.clone());
        }
        return List.copyOf(cloned);
    }
}
