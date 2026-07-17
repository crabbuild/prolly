package build.crab.prolly.javaapi;

import build.crab.prolly.MapChangeEventRecord;
import java.util.Optional;

/** Resumable, explicitly-polled versioned-map change subscription. */
public final class MapSubscription implements AutoCloseable {
    private build.crab.prolly.api.MapSubscription nativeSubscription;

    MapSubscription(build.crab.prolly.api.MapSubscription nativeSubscription) {
        this.nativeSubscription = nativeSubscription;
    }

    private build.crab.prolly.api.MapSubscription open() {
        if (nativeSubscription == null) throw new IllegalStateException("map subscription is closed");
        return nativeSubscription;
    }

    public Optional<byte[]> lastSeen() {
        byte[] value = open().lastSeen();
        return Optional.ofNullable(value == null ? null : value.clone());
    }

    public Optional<MapChangeEventRecord> poll() { return Optional.ofNullable(open().poll()); }

    @Override
    public void close() {
        if (nativeSubscription != null) {
            nativeSubscription.close();
            nativeSubscription = null;
        }
    }
}
