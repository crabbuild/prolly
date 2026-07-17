package build.crab.prolly.javaapi;

import build.crab.prolly.MapVersionRecord;
import build.crab.prolly.TreeRecord;
import java.util.Optional;

public record MapVersion(byte[] id, TreeRecord tree, Optional<Long> createdAtMillis, boolean head) {
    static MapVersion fromNative(MapVersionRecord value) {
        return new MapVersion(
                value.getId().clone(),
                value.getTree(),
                Optional.ofNullable(build.crab.prolly.api.JavaPortableBridge.mapVersionCreatedAtMillis(value)),
                value.isHead());
    }

    @Override
    public byte[] id() {
        return id.clone();
    }
}
