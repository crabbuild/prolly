package build.crab.prolly.javaapi;

import java.util.List;

public record VersionPrune(List<byte[]> retained, List<byte[]> removed) {
    static VersionPrune fromBridge(build.crab.prolly.api.JavaVersionPrune value) {
        return new VersionPrune(
                value.getRetained().stream().map(byte[]::clone).toList(),
                value.getRemoved().stream().map(byte[]::clone).toList());
    }
}
