package build.crab.prolly.javaapi;

import build.crab.prolly.MapVersionRecord;
import build.crab.prolly.TreeRecord;

public record MapVersion(byte[] id, TreeRecord tree, boolean head) {
    static MapVersion fromNative(MapVersionRecord value) {
        return new MapVersion(value.getId().clone(), value.getTree(), value.isHead());
    }

    @Override
    public byte[] id() {
        return id.clone();
    }
}
