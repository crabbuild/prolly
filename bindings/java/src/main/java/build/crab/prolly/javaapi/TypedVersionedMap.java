package build.crab.prolly.javaapi;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public final class TypedVersionedMap<K, V> {
    private final VersionedMap raw;
    private final KeyCodec<K> keyCodec;
    private final ValueCodec<V> valueCodec;

    TypedVersionedMap(VersionedMap raw, KeyCodec<K> keyCodec, ValueCodec<V> valueCodec) {
        this.raw = raw;
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
    }

    public VersionedMap raw() { return raw; }

    public Optional<V> get(K key) {
        return raw.getEncoded(keyCodec.encodeKey(key)).map(valueCodec::decode);
    }

    public Optional<V> getAt(byte[] id, K key) {
        return raw.getEncodedAt(id.clone(), keyCodec.encodeKey(key)).map(valueCodec::decode);
    }

    public List<TypedEntry<K, V>> entries() {
        return raw.encodedEntries().stream()
                .map(entry -> new TypedEntry<>(
                        keyCodec.decodeKey(entry.getKey()), valueCodec.decode(entry.getValue())))
                .toList();
    }

    public MapVersion put(K key, V value) {
        return raw.putEncoded(keyCodec.encodeKey(key), valueCodec.encode(value));
    }

    public MapUpdate putIf(byte[] expected, K key, V value) {
        return raw.putEncodedIf(
                expected == null ? null : expected.clone(),
                keyCodec.encodeKey(key), valueCodec.encode(value));
    }

    public MapVersion delete(K key) {
        return raw.deleteEncoded(keyCodec.encodeKey(key));
    }

    public <Old> TypedMigrationResult migrateFrom(
            byte[] expected,
            ValueCodec<Old> sourceCodec,
            Function<Old, V> migrate) {
        var entries = raw.encodedEntriesAt(expected.clone());
        var mutations = entries.stream().map(entry ->
                new build.crab.prolly.api.JavaMapMutation(
                        "upsert",
                        entry.getKey(),
                        valueCodec.encode(migrate.apply(sourceCodec.decode(entry.getValue())))))
                .toList();
        var update = raw.applyEncodedIf(expected.clone(), mutations);
        return new TypedMigrationResult(update, entries.size(), entries.size());
    }
}
