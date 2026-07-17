package build.crab.prolly.javaapi;

import build.crab.prolly.IndexEntryRecord;
import build.crab.prolly.SecondaryIndexExtractorCallback;
import build.crab.prolly.api.JavaPortableBridge;
import java.util.Objects;

public final class IndexRegistry implements AutoCloseable {
    private build.crab.prolly.api.IndexRegistry nativeRegistry;

    IndexRegistry(build.crab.prolly.api.IndexRegistry nativeRegistry) {
        this.nativeRegistry = nativeRegistry;
    }

    build.crab.prolly.api.IndexRegistry nativeRegistry() {
        if (nativeRegistry == null) throw new IllegalStateException("index registry is closed");
        return nativeRegistry;
    }

    public void register(
            byte[] name,
            long generation,
            String extractorId,
            IndexProjection projection,
            IndexExtractor extractor) {
        Objects.requireNonNull(extractor, "extractor");
        SecondaryIndexExtractorCallback callback = (primaryKey, sourceValue) ->
                extractor.extract(primaryKey.clone(), sourceValue.clone()).stream()
                        .map(entry -> new IndexEntryRecord(entry.term(), entry.projection()))
                        .toList();
        JavaPortableBridge.register(
                nativeRegistry(), name.clone(), generation, extractorId,
                Objects.requireNonNull(projection).nativeValue, callback);
    }

    @Override
    public void close() {
        if (nativeRegistry != null) {
            nativeRegistry.close();
            nativeRegistry = null;
        }
    }
}
