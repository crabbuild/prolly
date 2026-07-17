package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaCompositeAcceleratorConfig;
import build.crab.prolly.api.JavaPortableBridge;

public record CompositeAcceleratorConfig(
        long maxDeltaRecords,
        long maxShadowRecords,
        long maxDeltaRatioPpm,
        long maxShadowRatioPpm,
        long baseOverfetchMultiplier) {
    public static CompositeAcceleratorConfig defaults() {
        return fromNative(JavaPortableBridge.defaultCompositeAcceleratorConfig());
    }
    static CompositeAcceleratorConfig fromNative(JavaCompositeAcceleratorConfig value) {
        return new CompositeAcceleratorConfig(
                value.getMaxDeltaRecords(), value.getMaxShadowRecords(),
                value.getMaxDeltaRatioPpm(), value.getMaxShadowRatioPpm(),
                value.getBaseOverfetchMultiplier());
    }
    JavaCompositeAcceleratorConfig toNative() {
        return new JavaCompositeAcceleratorConfig(
                maxDeltaRecords, maxShadowRecords, maxDeltaRatioPpm,
                maxShadowRatioPpm, baseOverfetchMultiplier);
    }
}
