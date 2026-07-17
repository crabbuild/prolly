package build.crab.prolly.javaapi;

import java.util.List;
import java.util.Optional;

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
    @Override public void close() {
        if (nativeSession != null) { nativeSession.close(); nativeSession = null; }
    }
}
