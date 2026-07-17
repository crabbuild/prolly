package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaHnswConfig;
import build.crab.prolly.api.JavaPortableBridge;
import java.util.Objects;

public record HnswConfig(
        int maxConnections,
        long efConstruction,
        long efSearch,
        int levelBits,
        long overfetchMultiplier,
        long seed,
        RoutingVectorEncoding routingVectorEncoding) {
    public enum RoutingVectorEncoding { FULL_F32 }

    public HnswConfig {
        if (maxConnections < 0 || maxConnections > 65_535) {
            throw new IllegalArgumentException("maxConnections must fit an unsigned 16-bit value");
        }
        requireUInt(efConstruction, "efConstruction");
        requireUInt(efSearch, "efSearch");
        if (levelBits < 0 || levelBits > 255) {
            throw new IllegalArgumentException("levelBits must fit an unsigned 8-bit value");
        }
        requireUInt(overfetchMultiplier, "overfetchMultiplier");
        Objects.requireNonNull(routingVectorEncoding, "routingVectorEncoding");
    }

    public static HnswConfig defaults() {
        return fromNative(JavaPortableBridge.defaultHnswConfig());
    }

    static HnswConfig fromNative(JavaHnswConfig value) {
        return new HnswConfig(
                value.getMaxConnections(), value.getEfConstruction(), value.getEfSearch(),
                value.getLevelBits(), value.getOverfetchMultiplier(), value.getSeed(),
                RoutingVectorEncoding.valueOf(value.getRoutingVectorEncoding()));
    }

    JavaHnswConfig toNative() {
        return new JavaHnswConfig(
                maxConnections, efConstruction, efSearch, levelBits, overfetchMultiplier, seed,
                routingVectorEncoding.name());
    }

    private static void requireUInt(long value, String name) {
        if (value < 0 || value > 0xffff_ffffL) {
            throw new IllegalArgumentException(name + " must fit an unsigned 32-bit value");
        }
    }
}
