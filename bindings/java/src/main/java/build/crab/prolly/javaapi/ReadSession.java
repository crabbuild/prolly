package build.crab.prolly.javaapi;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

public final class ReadSession implements AutoCloseable {
    private build.crab.prolly.api.ReadSession nativeSession;
    ReadSession(build.crab.prolly.api.ReadSession nativeSession) { this.nativeSession = nativeSession; }
    private build.crab.prolly.api.ReadSession open() {
        if (nativeSession == null) throw new IllegalStateException("read session is closed");
        return nativeSession;
    }
    public Optional<byte[]> get(byte[] key) {
        byte[] value = open().get(key.clone());
        return Optional.ofNullable(value == null ? null : value.clone());
    }
    public List<byte[]> getMany(List<byte[]> keys) {
        return open().getMany(keys.stream().map(byte[]::clone).toList()).stream()
                .map(value -> value == null ? null : value.clone()).toList();
    }
    public ReadScanOutcome scanRangeView(
            byte[] start, byte[] end, Predicate<EntryView> visitor) {
        if (visitor == null) throw new NullPointerException("visitor");
        var outcome = open().scanRangeView(
                start.clone(), end == null ? null : end.clone(),
                value -> visitor.test(EntryView.fromNative(value)));
        return new ReadScanOutcome(outcome.getVisited(), outcome.getStopped());
    }
    @Override public void close() {
        if (nativeSession != null) { nativeSession.close(); nativeSession = null; }
    }
}
