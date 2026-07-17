package build.crab.prolly.javaapi;

public final class ProximityCancellationToken implements AutoCloseable {
    private build.crab.prolly.api.ProximityCancellationToken nativeToken;

    public ProximityCancellationToken() {
        nativeToken = new build.crab.prolly.api.ProximityCancellationToken();
    }

    build.crab.prolly.api.ProximityCancellationToken open() {
        if (nativeToken == null) {
            throw new IllegalStateException("proximity cancellation token is closed");
        }
        return nativeToken;
    }

    public void cancel() { open().cancel(); }
    public boolean isCancelled() { return open().isCancelled(); }

    @Override
    public void close() {
        if (nativeToken != null) {
            nativeToken.close();
            nativeToken = null;
        }
    }
}
