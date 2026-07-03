package build.crab.prolly;

public record SnapshotRoot(
        byte[] id,
        byte[] name,
        TreeRecord tree,
        Long createdAtMillis,
        Long updatedAtMillis) {
    public SnapshotRoot {
        id = id.clone();
        name = name.clone();
    }

    @Override
    public byte[] id() {
        return id.clone();
    }

    @Override
    public byte[] name() {
        return name.clone();
    }
}
