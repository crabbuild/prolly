package build.crab.prolly;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class Prolly implements AutoCloseable {
    private final ProllyEngine engine;

    private Prolly(ProllyEngine engine) {
        this.engine = engine;
    }

    public static Path useLocalDebugLibrary() {
        return ProllyNative.useLocalDebugLibrary();
    }

    public static void useLibrary(Path path) {
        ProllyNative.useLibrary(path);
    }

    public static Prolly memory() throws ProllyBindingException {
        return memory(ProllyKt.defaultConfig());
    }

    public static Prolly memory(ConfigRecord config) throws ProllyBindingException {
        return new Prolly(ProllyEngine.Companion.memory(config));
    }

    public static Prolly file(Path path) throws ProllyBindingException {
        return file(path, ProllyKt.defaultConfig());
    }

    public static Prolly file(Path path, ConfigRecord config) throws ProllyBindingException {
        return new Prolly(ProllyEngine.Companion.file(path.toString(), config));
    }

    public static Prolly sqlite(Path path) throws ProllyBindingException {
        return sqlite(path, ProllyKt.defaultConfig());
    }

    public static Prolly sqlite(Path path, ConfigRecord config) throws ProllyBindingException {
        return new Prolly(ProllyEngine.Companion.sqlite(path.toString(), config));
    }

    public static Prolly sqliteInMemory() throws ProllyBindingException {
        return sqliteInMemory(ProllyKt.defaultConfig());
    }

    public static Prolly sqliteInMemory(ConfigRecord config) throws ProllyBindingException {
        return new Prolly(ProllyEngine.Companion.sqliteInMemory(config));
    }

    public static Prolly customStore(HostStore store) throws ProllyBindingException {
        return customStore(store, ProllyKt.defaultConfig());
    }

    public static Prolly customStore(HostStore store, ConfigRecord config) throws ProllyBindingException {
        return new Prolly(ProllyEngine.Companion.customStore(new HostStoreAdapter(Objects.requireNonNull(store)), config));
    }

    public static ConfigRecord defaultConfig() {
        return ProllyKt.defaultConfig();
    }

    public static EncodingRecord encodingRaw() {
        return ProllyKt.encodingRaw();
    }

    public static EncodingRecord encodingCbor() {
        return ProllyKt.encodingCbor();
    }

    public static EncodingRecord encodingJson() {
        return ProllyKt.encodingJson();
    }

    public static EncodingRecord encodingCustom(String name) {
        return ProllyKt.encodingCustom(Objects.requireNonNull(name));
    }

    public static ConfigRecord config(
            long minChunkSize,
            long maxChunkSize,
            int chunkingFactor,
            long hashSeed,
            String encodingKind,
            String customEncodingName,
            Long nodeCacheMaxNodes,
            Long nodeCacheMaxBytes) {
        return ProllyJavaAdapters.config(
                minChunkSize,
                maxChunkSize,
                chunkingFactor,
                hashSeed,
                encodingKind,
                customEncodingName,
                nodeCacheMaxNodes,
                nodeCacheMaxBytes);
    }

    public static ConfigRecord treeConfig(
            long minChunkSize,
            long maxChunkSize,
            int chunkingFactor,
            long hashSeed,
            EncodingRecord encoding,
            Long nodeCacheMaxNodes,
            Long nodeCacheMaxBytes) throws ProllyBindingException {
        return ProllyJavaAdapters.treeConfig(
                minChunkSize,
                maxChunkSize,
                chunkingFactor,
                hashSeed,
                Objects.requireNonNull(encoding),
                nodeCacheMaxNodes,
                nodeCacheMaxBytes);
    }

    public static Long configNodeCacheMaxNodes(ConfigRecord record) {
        return ProllyJavaAdapters.configNodeCacheMaxNodes(Objects.requireNonNull(record));
    }

    public static byte[] cidFromBytes(byte[] bytes) {
        return ProllyKt.cidFromBytes(bytes.clone());
    }

    public static byte[] nodeBytesRoundTrip(byte[] bytes) throws ProllyBindingException {
        return ProllyKt.nodeToBytes(ProllyKt.nodeFromBytes(bytes.clone()));
    }

    public static byte[] nodeCidFromBytes(byte[] bytes) throws ProllyBindingException {
        return ProllyKt.nodeCid(ProllyKt.nodeFromBytes(bytes.clone()));
    }

    public static KeyProofVerification verifyKeyProof(KeyProof proof) throws ProllyBindingException {
        return KeyProofVerification.fromRecord(ProllyJavaAdapters.verifyKeyProof(proof.toRecord()));
    }

    public static KeyProof keyProofFromNodeBytes(byte[] root, byte[] key, List<byte[]> pathNodeBytes)
            throws ProllyBindingException {
        return KeyProof.fromRecord(
                ProllyJavaAdapters.keyProofFromNodeBytes(
                        root == null ? null : root.clone(),
                        key.clone(),
                        cloneByteArrays(pathNodeBytes)));
    }

    public static byte[] keyProofToBytes(KeyProof proof) throws ProllyBindingException {
        return ProllyJavaAdapters.keyProofToBytes(proof.toRecord()).clone();
    }

    public static KeyProof keyProofFromBytes(byte[] bytes) throws ProllyBindingException {
        return KeyProof.fromRecord(ProllyJavaAdapters.keyProofFromBytes(bytes.clone()));
    }

    public static MultiKeyProofVerification verifyMultiKeyProof(MultiKeyProof proof)
            throws ProllyBindingException {
        return MultiKeyProofVerification.fromRecord(ProllyJavaAdapters.verifyMultiKeyProof(proof.toRecord()));
    }

    public static MultiKeyProof multiKeyProofFromNodeBytes(
            byte[] root,
            List<byte[]> keys,
            List<byte[]> pathNodeBytes)
            throws ProllyBindingException {
        return MultiKeyProof.fromRecord(
                ProllyJavaAdapters.multiKeyProofFromNodeBytes(
                        root == null ? null : root.clone(),
                        cloneByteArrays(keys),
                        cloneByteArrays(pathNodeBytes)));
    }

    public static byte[] multiKeyProofToBytes(MultiKeyProof proof) throws ProllyBindingException {
        return ProllyJavaAdapters.multiKeyProofToBytes(proof.toRecord()).clone();
    }

    public static MultiKeyProof multiKeyProofFromBytes(byte[] bytes) throws ProllyBindingException {
        return MultiKeyProof.fromRecord(ProllyJavaAdapters.multiKeyProofFromBytes(bytes.clone()));
    }

    public static RangeProofVerification verifyRangeProof(RangeProof proof)
            throws ProllyBindingException {
        return RangeProofVerification.fromRecord(ProllyJavaAdapters.verifyRangeProof(proof.toRecord()));
    }

    public static RangePageProofVerification verifyRangePageProof(RangePageProof proof)
            throws ProllyBindingException {
        return RangePageProofVerification.fromRecord(ProllyJavaAdapters.verifyRangePageProof(proof.toRecord()));
    }

    public static DiffPageProofVerification verifyDiffPageProof(DiffPageProof proof)
            throws ProllyBindingException {
        return DiffPageProofVerification.fromRecord(ProllyJavaAdapters.verifyDiffPageProof(proof.toRecord()));
    }

    public static RangeProof rangeProofFromNodeBytes(
            byte[] root,
            byte[] start,
            byte[] end,
            List<byte[]> pathNodeBytes)
            throws ProllyBindingException {
        return RangeProof.fromRecord(
                ProllyJavaAdapters.rangeProofFromNodeBytes(
                        root == null ? null : root.clone(),
                        start.clone(),
                        end == null ? null : end.clone(),
                        cloneByteArrays(pathNodeBytes)));
    }

    public static byte[] rangeProofToBytes(RangeProof proof) throws ProllyBindingException {
        return ProllyJavaAdapters.rangeProofToBytes(proof.toRecord()).clone();
    }

    public static RangeProof rangeProofFromBytes(byte[] bytes) throws ProllyBindingException {
        return RangeProof.fromRecord(ProllyJavaAdapters.rangeProofFromBytes(bytes.clone()));
    }

    public static RangePageProof rangePageProofFromNodeBytes(
            byte[] root,
            byte[] after,
            byte[] end,
            List<byte[]> pathNodeBytes)
            throws ProllyBindingException {
        return RangePageProof.fromRecord(
                ProllyJavaAdapters.rangePageProofFromNodeBytes(
                        root == null ? null : root.clone(),
                        after == null ? null : after.clone(),
                        end == null ? null : end.clone(),
                        cloneByteArrays(pathNodeBytes)));
    }

    public static byte[] rangePageProofToBytes(RangePageProof proof) throws ProllyBindingException {
        return ProllyJavaAdapters.rangePageProofToBytes(proof.toRecord()).clone();
    }

    public static RangePageProof rangePageProofFromBytes(byte[] bytes) throws ProllyBindingException {
        return RangePageProof.fromRecord(ProllyJavaAdapters.rangePageProofFromBytes(bytes.clone()));
    }

    public static byte[] diffPageProofToBytes(DiffPageProof proof) throws ProllyBindingException {
        return ProllyJavaAdapters.diffPageProofToBytes(proof.toRecord()).clone();
    }

    public static DiffPageProof diffPageProofFromBytes(byte[] bytes) throws ProllyBindingException {
        return DiffPageProof.fromRecord(ProllyJavaAdapters.diffPageProofFromBytes(bytes.clone()));
    }

    public static ProofBundleSummary inspectProofBundle(byte[] bytes) throws ProllyBindingException {
        return ProofBundleSummary.fromRecord(ProllyJavaAdapters.inspectProofBundle(bytes.clone()));
    }

    public static ProofBundleVerification verifyProofBundle(byte[] bytes) throws ProllyBindingException {
        return ProofBundleVerification.fromRecord(ProllyJavaAdapters.verifyProofBundle(bytes.clone()));
    }

    public static AuthenticatedProofEnvelope signProofBundleHmacSha256(
            byte[] proofBundle,
            byte[] keyId,
            byte[] secret,
            byte[] context,
            Long issuedAtMillis,
            Long expiresAtMillis,
            byte[] nonce)
            throws ProllyBindingException {
        return AuthenticatedProofEnvelope.fromRecord(
                ProllyJavaAdapters.signProofBundleHmacSha256(
                        proofBundle.clone(),
                        keyId.clone(),
                        secret.clone(),
                        context.clone(),
                        issuedAtMillis,
                        expiresAtMillis,
                        nonce.clone()));
    }

    public static AuthenticatedProofEnvelopeVerification verifyAuthenticatedProofEnvelope(
            AuthenticatedProofEnvelope envelope,
            byte[] secret,
            Long nowMillis) {
        return AuthenticatedProofEnvelopeVerification.fromRecord(
                ProllyJavaAdapters.verifyAuthenticatedProofEnvelope(
                        envelope.toRecord(),
                        secret.clone(),
                        nowMillis));
    }

    public static AuthenticatedProofBundleVerification verifyAuthenticatedProofBundle(
            byte[] envelopeBytes,
            byte[] secret,
            Long nowMillis) throws ProllyBindingException {
        return AuthenticatedProofBundleVerification.fromRecord(
                ProllyJavaAdapters.verifyAuthenticatedProofBundle(
                        envelopeBytes.clone(),
                        secret.clone(),
                        nowMillis));
    }

    public static byte[] authenticatedProofEnvelopeToBytes(AuthenticatedProofEnvelope envelope)
            throws ProllyBindingException {
        return ProllyJavaAdapters.authenticatedProofEnvelopeToBytes(envelope.toRecord()).clone();
    }

    public static AuthenticatedProofEnvelope authenticatedProofEnvelopeFromBytes(byte[] bytes)
            throws ProllyBindingException {
        return AuthenticatedProofEnvelope.fromRecord(
                ProllyJavaAdapters.authenticatedProofEnvelopeFromBytes(bytes.clone()));
    }

    public static boolean isBoundaryConfig(ConfigRecord config, long count, byte[] key, byte[] value)
            throws ProllyBindingException {
        return ProllyJavaAdapters.isBoundaryConfig(config, count, key.clone(), value.clone());
    }

    public static byte[] prefixEnd(byte[] prefix) {
        byte[] result = ProllyKt.prefixEnd(prefix.clone());
        return result == null ? null : result.clone();
    }

    public static RangeBoundsRecord prefixRange(byte[] prefix) {
        return ProllyKt.prefixRange(prefix.clone());
    }

    public static RangeCursorRecord rangeCursorStart() {
        return ProllyKt.rangeCursorStart();
    }

    public static RangeCursorRecord rangeCursorAfterKey(byte[] key) {
        return ProllyKt.rangeCursorAfterKey(key.clone());
    }

    public static ReverseCursorRecord reverseCursorEnd() {
        return ProllyKt.reverseCursorEnd();
    }

    public static ReverseCursorRecord reverseCursorBeforeKey(byte[] key) {
        return ProllyKt.reverseCursorBeforeKey(key.clone());
    }

    public static byte[] u64Key(String value) {
        return ProllyJavaAdapters.u64Key(value);
    }

    public static byte[] u128Key(String value) {
        return ProllyJavaAdapters.u128Key(value);
    }

    public static byte[] i64Key(long value) {
        return ProllyKt.i64Key(value);
    }

    public static byte[] i128Key(String value) {
        return ProllyJavaAdapters.i128Key(value);
    }

    public static byte[] timestampMillisKey(String value) {
        return ProllyJavaAdapters.timestampMillisKey(value);
    }

    public static byte[] encodeSegment(byte[] segment) {
        return ProllyKt.encodeSegment(segment.clone());
    }

    public static byte[] keyFromSegments(List<byte[]> segments) {
        return ProllyKt.keyFromSegments(cloneByteArrays(segments));
    }

    public static byte[] keyFromPrefixedSegments(byte[] prefix, List<byte[]> segments) {
        return ProllyKt.keyFromPrefixedSegments(prefix.clone(), cloneByteArrays(segments));
    }

    public static ChangedSpanRecord changedSpan(byte[] start, byte[] end) {
        return ProllyKt.changedSpan(start.clone(), end == null ? null : end.clone());
    }

    public static ChangedSpanRecord changedSpanFromKey(byte[] key) {
        return ProllyKt.changedSpanFromKey(key.clone());
    }

    public static ChangedSpanRecord changedSpanForPrefix(byte[] prefix) {
        return ProllyKt.changedSpanForPrefix(prefix.clone());
    }

    public static List<byte[]> decodeSegments(byte[] key) throws ProllyBindingException {
        return ProllyKt.decodeSegments(key.clone());
    }

    public static String debugKey(byte[] key) {
        return ProllyKt.debugKey(key.clone());
    }

    public static byte[] versionedValueBytesRoundTrip(byte[] bytes) throws ProllyBindingException {
        return ProllyKt.versionedValueToBytes(ProllyKt.versionedValueFromBytes(bytes.clone()));
    }

    public static boolean versionedValueBytesMatchesSchema(byte[] bytes, String schema, long version)
            throws ProllyBindingException {
        return ProllyJavaAdapters.versionedValueBytesMatchesSchema(bytes.clone(), schema, version);
    }

    public static void versionedValueBytesRequireSchema(byte[] bytes, String schema, long version)
            throws ProllyBindingException {
        ProllyJavaAdapters.versionedValueBytesRequireSchema(bytes.clone(), schema, version);
    }

    public static byte[] valueRefBytesRoundTrip(byte[] bytes) throws ProllyBindingException {
        return ProllyKt.valueRefToBytes(ProllyKt.valueRefFromBytes(bytes.clone()));
    }

    public static ValueRef valueRefFromStoredBytes(byte[] bytes) throws ProllyBindingException {
        return new ValueRef(ProllyKt.valueRefFromStoredBytes(bytes.clone()));
    }

    public static boolean valueRefInlineRequiresEscape(byte[] value) {
        return ProllyKt.valueRefInlineRequiresEscape(value.clone());
    }

    public static void blobRefValidateBytes(BlobRef reference, byte[] bytes) throws ProllyBindingException {
        ProllyKt.blobRefValidateBytes(reference.toRecord(), bytes.clone());
    }

    public static byte[] rootManifestBytesRoundTrip(byte[] bytes) throws ProllyBindingException {
        return ProllyKt.rootManifestToBytes(ProllyKt.rootManifestFromBytes(bytes.clone()));
    }

    public static MutationRecord upsert(byte[] key, byte[] value) {
        return upsertMutation(key, value);
    }

    public static MutationRecord upsertMutation(byte[] key, byte[] value) {
        return ProllyKt.upsertMutation(key.clone(), value.clone());
    }

    public static MutationRecord deleteMutation(byte[] key) {
        return ProllyKt.deleteMutation(key.clone());
    }

    public static ResolutionRecord resolutionValue(byte[] value) {
        return ProllyKt.resolutionValue(value.clone());
    }

    public static ResolutionRecord resolutionDelete() {
        return ProllyKt.resolutionDelete();
    }

    public static ResolutionRecord resolutionUnresolved() {
        return ProllyKt.resolutionUnresolved();
    }

    public static ResolutionRecord resolvePreferLeft(ConflictRecord conflict) {
        return ProllyKt.resolvePreferLeft(conflict);
    }

    public static ResolutionRecord resolvePreferRight(ConflictRecord conflict) {
        return ProllyKt.resolvePreferRight(conflict);
    }

    public static ResolutionRecord resolveDeleteWins(ConflictRecord conflict) {
        return ProllyKt.resolveDeleteWins(conflict);
    }

    public static ResolutionRecord resolveUpdateWins(ConflictRecord conflict) {
        return ProllyKt.resolveUpdateWins(conflict);
    }

    public static CrdtResolutionRecord crdtResolutionValue(byte[] value) {
        return ProllyKt.crdtResolutionValue(value.clone());
    }

    public static CrdtResolutionRecord crdtResolutionDelete() {
        return ProllyKt.crdtResolutionDelete();
    }

    public static ParallelConfigRecord parallelConfig(long maxThreads, long parallelismThreshold) {
        return ProllyJavaAdapters.parallelConfig(maxThreads, parallelismThreshold);
    }

    public static ParallelConfigRecord parallelConfigSequential() {
        return ProllyJavaAdapters.parallelConfigSequential();
    }

    public static long parallelConfigMaxThreads(ParallelConfigRecord record) {
        return ProllyJavaAdapters.parallelConfigMaxThreads(Objects.requireNonNull(record));
    }

    public static LargeValueConfig largeValueConfig(long inlineThreshold) {
        return new LargeValueConfig(inlineThreshold);
    }

    public static LargeValueConfig defaultLargeValueConfig() {
        return new LargeValueConfig(ProllyKt.defaultLargeValueConfig());
    }

    public static CrdtConfigRecord crdtConfigLww(String deletePolicy) {
        return ProllyJavaAdapters.crdtConfigLww(deletePolicy);
    }

    public static CrdtConfigRecord crdtConfigMultiValue(String deletePolicy) {
        return ProllyJavaAdapters.crdtConfigMultiValue(deletePolicy);
    }

    public static TimestampedValueRecord timestampedValue(byte[] value, long timestamp) {
        return ProllyJavaAdapters.timestampedValue(value.clone(), timestamp);
    }

    public static byte[] timestampedValueToBytes(TimestampedValueRecord record) {
        return ProllyKt.timestampedValueToBytes(record);
    }

    public static TimestampedValueRecord timestampedValueFromBytes(byte[] bytes) throws ProllyBindingException {
        return ProllyKt.timestampedValueFromBytes(bytes.clone());
    }

    public static TimestampedValueRecord timestampedValueNow(byte[] value) {
        return ProllyKt.timestampedValueNow(value.clone());
    }

    public static long timestampedValueTimestamp(TimestampedValueRecord record) {
        return ProllyJavaAdapters.timestampedValueTimestamp(record);
    }

    public static byte[] multiValueSetToBytes(List<byte[]> values) {
        return ProllyKt.multiValueSetToBytes(cloneByteArrays(values));
    }

    public static List<byte[]> multiValueSetFromBytes(byte[] bytes) throws ProllyBindingException {
        return cloneByteArrays(ProllyKt.multiValueSetFromBytes(bytes.clone()));
    }

    public static List<byte[]> multiValueSetMerge(List<byte[]> left, List<byte[]> right) {
        return cloneByteArrays(ProllyKt.multiValueSetMerge(cloneByteArrays(left), cloneByteArrays(right)));
    }

    public static TombstoneMetadataRecord tombstoneMetadata(String key, byte[] value) {
        return ProllyJavaAdapters.tombstoneMetadata(key, value.clone());
    }

    public static TombstoneRecord tombstone(
            byte[] actor,
            long timestampMillis,
            List<TombstoneMetadataRecord> causalMetadata) {
        return ProllyJavaAdapters.tombstone(actor.clone(), timestampMillis, cloneTombstoneMetadata(causalMetadata));
    }

    public static long tombstoneTimestampMillis(TombstoneRecord record) {
        return ProllyJavaAdapters.tombstoneTimestampMillis(record);
    }

    public static byte[] tombstoneToBytes(TombstoneRecord record) throws ProllyBindingException {
        return ProllyKt.tombstoneToBytes(record);
    }

    public static TombstoneRecord tombstoneFromBytes(byte[] bytes) throws ProllyBindingException {
        return ProllyKt.tombstoneFromBytes(bytes.clone());
    }

    public static Optional<TombstoneRecord> tombstoneFromStoredBytes(byte[] bytes) throws ProllyBindingException {
        return Optional.ofNullable(ProllyKt.tombstoneFromStoredBytes(bytes.clone()));
    }

    public static boolean isTombstoneValue(byte[] bytes) {
        return ProllyKt.isTombstoneValue(bytes.clone());
    }

    public static MutationRecord tombstoneUpsertMutation(byte[] key, TombstoneRecord tombstone)
            throws ProllyBindingException {
        return ProllyKt.tombstoneUpsertMutation(key.clone(), tombstone);
    }

    public static Optional<MutationRecord> tombstoneCompactionMutation(byte[] key, byte[] storedValue)
            throws ProllyBindingException {
        return Optional.ofNullable(ProllyKt.tombstoneCompactionMutation(key.clone(), storedValue.clone()));
    }

    public static NamedRootRetentionRecord retainAllNamedRoots() {
        return ProllyJavaAdapters.retentionAll();
    }

    public static NamedRootRetentionRecord retainExactNamedRoots(List<byte[]> names) {
        return ProllyJavaAdapters.retentionExact(cloneByteArrays(names));
    }

    public static NamedRootRetentionRecord retainNamedRootPrefix(byte[] prefix) {
        return ProllyJavaAdapters.retentionPrefix(prefix.clone());
    }

    public static NamedRootRetentionRecord retainNewestNamedRoots(long count) {
        return retainNewestNamedRoots(new byte[0], count);
    }

    public static NamedRootRetentionRecord retainNewestNamedRoots(byte[] prefix, long count) {
        return ProllyJavaAdapters.retentionNewestByName(prefix.clone(), count);
    }

    public static NamedRootRetentionRecord retainNamedRootsUpdatedSince(long minUpdatedAtMillis) {
        return retainNamedRootsUpdatedSince(new byte[0], minUpdatedAtMillis);
    }

    public static NamedRootRetentionRecord retainNamedRootsUpdatedSince(byte[] prefix, long minUpdatedAtMillis) {
        return ProllyJavaAdapters.retentionUpdatedSince(prefix.clone(), minUpdatedAtMillis);
    }

    public static SnapshotNamespaceRecord snapshotNamespaceBranch() {
        return ProllyJavaAdapters.snapshotNamespaceBranch();
    }

    public static SnapshotNamespaceRecord snapshotNamespaceTag() {
        return ProllyJavaAdapters.snapshotNamespaceTag();
    }

    public static SnapshotNamespaceRecord snapshotNamespaceCheckpoint() {
        return ProllyJavaAdapters.snapshotNamespaceCheckpoint();
    }

    public static SnapshotNamespaceRecord snapshotNamespaceCustom(byte[] prefix) {
        return ProllyJavaAdapters.snapshotNamespaceCustom(prefix.clone());
    }

    public static byte[] snapshotRootName(SnapshotNamespaceRecord namespace, byte[] id)
            throws ProllyBindingException {
        return ProllyJavaAdapters.snapshotRootName(cloneSnapshotNamespace(namespace), id.clone());
    }

    public static Optional<byte[]> snapshotIdFromName(SnapshotNamespaceRecord namespace, byte[] name)
            throws ProllyBindingException {
        byte[] id = ProllyJavaAdapters.snapshotIdFromName(cloneSnapshotNamespace(namespace), name.clone());
        return Optional.ofNullable(id == null ? null : id.clone());
    }

    public static long snapshotBundleFormatVersion(SnapshotBundleRecord record) {
        return ProllyJavaAdapters.snapshotBundleFormatVersion(Objects.requireNonNull(record));
    }

    public static long snapshotBundleNodeCount(SnapshotBundleRecord record) {
        return ProllyJavaAdapters.snapshotBundleNodeCount(Objects.requireNonNull(record));
    }

    public static byte[] snapshotBundleToBytes(SnapshotBundleRecord record) throws ProllyBindingException {
        return ProllyKt.snapshotBundleToBytes(Objects.requireNonNull(record)).clone();
    }

    public static SnapshotBundleRecord snapshotBundleFromBytes(byte[] bytes) throws ProllyBindingException {
        return ProllyKt.snapshotBundleFromBytes(bytes.clone());
    }

    public static byte[] snapshotBundleDigest(SnapshotBundleRecord record) throws ProllyBindingException {
        return ProllyKt.snapshotBundleDigest(Objects.requireNonNull(record)).clone();
    }

    public static byte[] snapshotBundleDigestBytes(byte[] bytes) throws ProllyBindingException {
        return ProllyKt.snapshotBundleDigestBytes(bytes.clone()).clone();
    }

    public static SnapshotBundleSummaryRecord snapshotBundleSummary(SnapshotBundleRecord record)
            throws ProllyBindingException {
        return ProllyKt.snapshotBundleSummary(Objects.requireNonNull(record));
    }

    public static SnapshotBundleSummaryRecord snapshotBundleSummaryFromBytes(byte[] bytes)
            throws ProllyBindingException {
        return ProllyKt.snapshotBundleSummaryFromBytes(bytes.clone());
    }

    public static SnapshotBundleVerificationRecord verifySnapshotBundle(SnapshotBundleRecord record)
            throws ProllyBindingException {
        return ProllyKt.verifySnapshotBundle(Objects.requireNonNull(record));
    }

    public static SnapshotBundleVerificationRecord verifySnapshotBundleBytes(byte[] bytes)
            throws ProllyBindingException {
        return ProllyKt.verifySnapshotBundleBytes(bytes.clone());
    }

    public static long snapshotBundleSummaryFormatVersion(SnapshotBundleSummaryRecord record) {
        return ProllyJavaAdapters.snapshotBundleSummaryFormatVersion(Objects.requireNonNull(record));
    }

    public static long snapshotBundleSummaryNodeCount(SnapshotBundleSummaryRecord record) {
        return ProllyJavaAdapters.snapshotBundleSummaryNodeCount(Objects.requireNonNull(record));
    }

    public static long snapshotBundleSummaryByteCount(SnapshotBundleSummaryRecord record) {
        return ProllyJavaAdapters.snapshotBundleSummaryByteCount(Objects.requireNonNull(record));
    }

    public static boolean snapshotBundleVerificationValid(SnapshotBundleVerificationRecord record) {
        return ProllyJavaAdapters.snapshotBundleVerificationValid(Objects.requireNonNull(record));
    }

    public static long snapshotBundleVerificationMissingCidCount(SnapshotBundleVerificationRecord record) {
        return ProllyJavaAdapters.snapshotBundleVerificationMissingCidCount(Objects.requireNonNull(record));
    }

    public static long snapshotBundleVerificationExtraCidCount(SnapshotBundleVerificationRecord record) {
        return ProllyJavaAdapters.snapshotBundleVerificationExtraCidCount(Objects.requireNonNull(record));
    }

    public static long treeStatsNumNodes(TreeStatsRecord record) {
        return ProllyJavaAdapters.treeStatsNumNodes(Objects.requireNonNull(record));
    }

    public static long treeStatsTotalKeyValuePairs(TreeStatsRecord record) {
        return ProllyJavaAdapters.treeStatsTotalKeyValuePairs(Objects.requireNonNull(record));
    }

    public static long treeStatsLevelCount(TreeStatsRecord record, int level) {
        return ProllyJavaAdapters.treeStatsLevelCount(Objects.requireNonNull(record), level);
    }

    public static long statsComparisonBeforeTotalKeyValuePairs(StatsComparisonRecord record) {
        return ProllyJavaAdapters.statsComparisonBeforeTotalKeyValuePairs(Objects.requireNonNull(record));
    }

    public static long statsComparisonAfterTotalKeyValuePairs(StatsComparisonRecord record) {
        return ProllyJavaAdapters.statsComparisonAfterTotalKeyValuePairs(Objects.requireNonNull(record));
    }

    public static long statsDiffTotalKeyValuePairs(StatsComparisonRecord record) {
        return ProllyJavaAdapters.statsDiffTotalKeyValuePairs(Objects.requireNonNull(record));
    }

    public static long treeDebugViewLevelCount(TreeDebugViewRecord record) {
        return ProllyJavaAdapters.treeDebugViewLevelCount(Objects.requireNonNull(record));
    }

    public static long treeDebugViewFirstLevelNodeCount(TreeDebugViewRecord record) {
        return ProllyJavaAdapters.treeDebugViewFirstLevelNodeCount(Objects.requireNonNull(record));
    }

    public static long treeDebugComparisonLeftOnlyNodes(TreeDebugComparisonRecord record) {
        return ProllyJavaAdapters.treeDebugComparisonLeftOnlyNodes(Objects.requireNonNull(record));
    }

    public static long treeDebugComparisonRightOnlyNodes(TreeDebugComparisonRecord record) {
        return ProllyJavaAdapters.treeDebugComparisonRightOnlyNodes(Objects.requireNonNull(record));
    }

    public static boolean treeDebugComparisonHasRightOnlyNode(TreeDebugComparisonRecord record) {
        return ProllyJavaAdapters.treeDebugComparisonHasRightOnlyNode(Objects.requireNonNull(record));
    }

    public static MergePolicyRegistry mergePolicyRegistry() {
        return new MergePolicyRegistry();
    }

    public TreeRecord create() {
        return engine.create();
    }

    public Optional<byte[]> get(TreeRecord tree, byte[] key) throws ProllyBindingException {
        return Optional.ofNullable(engine.get(tree, key.clone())).map(byte[]::clone);
    }

    public Optional<ValueRef> getValueRef(TreeRecord tree, byte[] key) throws ProllyBindingException {
        ValueRefRecord valueRef = engine.getValueRef(tree, key.clone());
        return valueRef == null ? Optional.empty() : Optional.of(new ValueRef(valueRef));
    }

    public Optional<byte[]> getLargeValue(BlobStore blobStore, TreeRecord tree, byte[] key)
            throws ProllyBindingException {
        byte[] value = engine.getLargeValue(blobStore.inner(), tree, key.clone());
        return value == null ? Optional.empty() : Optional.of(value.clone());
    }

    public List<byte[]> getMany(TreeRecord tree, List<byte[]> keys) throws ProllyBindingException {
        List<byte[]> values = engine.getMany(tree, cloneByteArrays(keys));
        List<byte[]> cloned = new ArrayList<>(values.size());
        for (byte[] value : values) {
            cloned.add(value == null ? null : value.clone());
        }
        return cloned;
    }

    public KeyProof proveKey(TreeRecord tree, byte[] key) throws ProllyBindingException {
        return KeyProof.fromRecord(engine.proveKey(tree, key.clone()));
    }

    public MultiKeyProof proveKeys(TreeRecord tree, List<byte[]> keys) throws ProllyBindingException {
        return MultiKeyProof.fromRecord(engine.proveKeys(tree, cloneByteArrays(keys)));
    }

    public RangeProof proveRange(TreeRecord tree, byte[] start, Optional<byte[]> end)
            throws ProllyBindingException {
        return RangeProof.fromRecord(engine.proveRange(tree, start.clone(), end.map(byte[]::clone).orElse(null)));
    }

    public RangeProof provePrefix(TreeRecord tree, byte[] prefix) throws ProllyBindingException {
        return RangeProof.fromRecord(engine.provePrefix(tree, prefix.clone()));
    }

    public ProvedRangePage proveRangePage(
            TreeRecord tree,
            RangeCursorRecord cursor,
            Optional<byte[]> end,
            long limit) throws ProllyBindingException {
        return ProvedRangePage.fromRecord(
                ProllyJavaAdapters.proveRangePage(engine, tree, cursor, end.map(byte[]::clone).orElse(null), limit));
    }

    public TreeRecord put(TreeRecord tree, byte[] key, byte[] value) throws ProllyBindingException {
        return engine.put(tree, key.clone(), value.clone());
    }

    public TreeRecord putLargeValue(
            BlobStore blobStore,
            TreeRecord tree,
            byte[] key,
            byte[] value,
            LargeValueConfig config) throws ProllyBindingException {
        return engine.putLargeValue(blobStore.inner(), tree, key.clone(), value.clone(), config.toRecord());
    }

    public TreeRecord delete(TreeRecord tree, byte[] key) throws ProllyBindingException {
        return engine.delete(tree, key.clone());
    }

    public TreeRecord batch(TreeRecord tree, List<MutationRecord> mutations) throws ProllyBindingException {
        return engine.batch(tree, cloneMutations(mutations));
    }

    public BatchApplyResult batchWithStats(TreeRecord tree, List<MutationRecord> mutations) throws ProllyBindingException {
        return new BatchApplyResult(engine.batchWithStats(tree, cloneMutations(mutations)));
    }

    public TreeRecord buildFromEntries(List<Entry> entries) throws ProllyBindingException {
        return engine.buildFromEntries(entryRecords(entries));
    }

    public TreeRecord buildFromSortedEntries(List<Entry> entries) throws ProllyBindingException {
        return engine.buildFromSortedEntries(entryRecords(entries));
    }

    public TreeRecord appendBatch(TreeRecord tree, List<MutationRecord> mutations) throws ProllyBindingException {
        return engine.appendBatch(tree, cloneMutations(mutations));
    }

    public BatchApplyResult appendBatchWithStats(TreeRecord tree, List<MutationRecord> mutations) throws ProllyBindingException {
        return new BatchApplyResult(engine.appendBatchWithStats(tree, cloneMutations(mutations)));
    }

    public TreeRecord parallelBatch(
            TreeRecord tree,
            List<MutationRecord> mutations,
            ParallelConfigRecord config) throws ProllyBindingException {
        return engine.parallelBatch(tree, cloneMutations(mutations), config);
    }

    public BatchApplyResult parallelBatchWithStats(
            TreeRecord tree,
            List<MutationRecord> mutations,
            ParallelConfigRecord config) throws ProllyBindingException {
        return new BatchApplyResult(engine.parallelBatchWithStats(tree, cloneMutations(mutations), config));
    }

    public Optional<Entry> firstEntry(TreeRecord tree) throws ProllyBindingException {
        return Optional.ofNullable(ProllyJavaAdapters.firstEntry(engine, tree))
                .map(Prolly::entry);
    }

    public Optional<Entry> lastEntry(TreeRecord tree) throws ProllyBindingException {
        return Optional.ofNullable(ProllyJavaAdapters.lastEntry(engine, tree))
                .map(Prolly::entry);
    }

    public Optional<Entry> lowerBound(TreeRecord tree, byte[] key) throws ProllyBindingException {
        return Optional.ofNullable(ProllyJavaAdapters.lowerBound(engine, tree, key.clone()))
                .map(Prolly::entry);
    }

    public Optional<Entry> upperBound(TreeRecord tree, byte[] key) throws ProllyBindingException {
        return Optional.ofNullable(ProllyJavaAdapters.upperBound(engine, tree, key.clone()))
                .map(Prolly::entry);
    }

    public List<Entry> prefix(TreeRecord tree, byte[] prefix) throws ProllyBindingException {
        return ProllyJavaAdapters.prefix(engine, tree, prefix.clone())
                .stream()
                .map(Prolly::entry)
                .toList();
    }

    public RangePageRecord prefixPage(TreeRecord tree, byte[] prefix, RangeCursorRecord cursor, long limit)
            throws ProllyBindingException {
        return ProllyJavaAdapters.prefixPage(engine, tree, prefix.clone(), cursor, limit);
    }

    public ReversePageRecord prefixReversePage(TreeRecord tree, byte[] prefix, ReverseCursorRecord cursor, long limit)
            throws ProllyBindingException {
        return ProllyJavaAdapters.prefixReversePage(engine, tree, prefix.clone(), cursor, limit);
    }

    public List<Entry> range(TreeRecord tree, byte[] start, Optional<byte[]> end)
            throws ProllyBindingException {
        return engine.range(tree, start.clone(), end.map(byte[]::clone).orElse(null))
                .stream()
                .map(Prolly::entry)
                .toList();
    }

    public List<Entry> rangeAfter(TreeRecord tree, byte[] afterKey, Optional<byte[]> end)
            throws ProllyBindingException {
        return engine.rangeAfter(tree, afterKey.clone(), end.map(byte[]::clone).orElse(null))
                .stream()
                .map(Prolly::entry)
                .toList();
    }

    public List<Entry> rangeFromCursor(TreeRecord tree, RangeCursorRecord cursor, Optional<byte[]> end)
            throws ProllyBindingException {
        return engine.rangeFromCursor(tree, cursor, end.map(byte[]::clone).orElse(null))
                .stream()
                .map(Prolly::entry)
                .toList();
    }

    public RangePageRecord rangePage(
            TreeRecord tree,
            RangeCursorRecord cursor,
            Optional<byte[]> end,
            long limit) throws ProllyBindingException {
        return ProllyJavaAdapters.rangePage(engine, tree, cursor, end.map(byte[]::clone).orElse(null), limit);
    }

    public ReversePageRecord reversePage(
            TreeRecord tree,
            ReverseCursorRecord cursor,
            byte[] start,
            long limit) throws ProllyBindingException {
        return ProllyJavaAdapters.reversePage(engine, tree, cursor, start.clone(), limit);
    }

    public CursorWindowRecord cursorWindow(
            TreeRecord tree,
            byte[] key,
            Optional<byte[]> end,
            long limit) throws ProllyBindingException {
        return ProllyJavaAdapters.cursorWindow(engine, tree, key.clone(), end.map(byte[]::clone).orElse(null), limit);
    }

    public List<DiffRecord> diff(TreeRecord base, TreeRecord other) throws ProllyBindingException {
        return engine.diff(base, other);
    }

    public List<DiffRecord> rangeDiff(TreeRecord base, TreeRecord other, byte[] start, Optional<byte[]> end)
            throws ProllyBindingException {
        return engine.rangeDiff(base, other, start.clone(), end.map(byte[]::clone).orElse(null));
    }

    public List<DiffRecord> diffFromCursor(
            TreeRecord base,
            TreeRecord other,
            RangeCursorRecord cursor,
            Optional<byte[]> end) throws ProllyBindingException {
        return engine.diffFromCursor(base, other, cursor, end.map(byte[]::clone).orElse(null));
    }

    public DiffPageRecord diffPage(
            TreeRecord base,
            TreeRecord other,
            RangeCursorRecord cursor,
            Optional<byte[]> end,
            long limit) throws ProllyBindingException {
        return ProllyJavaAdapters.diffPage(engine, base, other, cursor, end.map(byte[]::clone).orElse(null), limit);
    }

    public ProvedDiffPage proveDiffPage(
            TreeRecord base,
            TreeRecord other,
            RangeCursorRecord cursor,
            Optional<byte[]> end,
            long limit) throws ProllyBindingException {
        return ProvedDiffPage.fromRecord(
                ProllyJavaAdapters.proveDiffPage(engine, base, other, cursor, end.map(byte[]::clone).orElse(null), limit));
    }

    public ConflictPageRecord conflictPage(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            RangeCursorRecord cursor,
            long limit) throws ProllyBindingException {
        return ProllyJavaAdapters.conflictPage(engine, base, left, right, cursor, limit);
    }

    public TreeRecord merge(TreeRecord base, TreeRecord left, TreeRecord right, String resolver)
            throws ProllyBindingException {
        return engine.merge(base, left, right, resolver);
    }

    public TreeRecord mergeWithResolver(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            MergeResolverCallback resolver) throws ProllyBindingException {
        return engine.mergeWithResolver(base, left, right, resolver);
    }

    public TreeRecord mergeWithPolicy(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            MergePolicyRegistry policy) throws ProllyBindingException {
        return engine.mergeWithPolicy(base, left, right, policy);
    }

    public TreeRecord crdtMerge(TreeRecord base, TreeRecord left, TreeRecord right, CrdtConfigRecord config)
            throws ProllyBindingException {
        return engine.crdtMerge(base, left, right, config);
    }

    public TreeRecord crdtMergeWithResolver(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            CrdtDeletePolicyKind deletePolicy,
            CrdtResolverCallback resolver) throws ProllyBindingException {
        return engine.crdtMergeWithResolver(base, left, right, deletePolicy, resolver);
    }

    public MergeExplanationRecord mergeExplain(TreeRecord base, TreeRecord left, TreeRecord right, String resolver)
            throws ProllyBindingException {
        return engine.mergeExplain(base, left, right, resolver);
    }

    public MergeExplanationRecord mergeExplainWithResolver(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            MergeResolverCallback resolver) throws ProllyBindingException {
        return engine.mergeExplainWithResolver(base, left, right, resolver);
    }

    public MergeExplanationRecord mergeExplainWithPolicy(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            MergePolicyRegistry policy) throws ProllyBindingException {
        return engine.mergeExplainWithPolicy(base, left, right, policy);
    }

    public TreeRecord mergeRange(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            byte[] start,
            Optional<byte[]> end,
            String resolver) throws ProllyBindingException {
        return engine.mergeRange(base, left, right, start.clone(), end.map(byte[]::clone).orElse(null), resolver);
    }

    public TreeRecord mergeRangeWithResolver(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            byte[] start,
            Optional<byte[]> end,
            MergeResolverCallback resolver) throws ProllyBindingException {
        return engine.mergeRangeWithResolver(base, left, right, start.clone(), end.map(byte[]::clone).orElse(null), resolver);
    }

    public TreeRecord mergeRangeWithPolicy(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            byte[] start,
            Optional<byte[]> end,
            MergePolicyRegistry policy) throws ProllyBindingException {
        return engine.mergeRangeWithPolicy(base, left, right, start.clone(), end.map(byte[]::clone).orElse(null), policy);
    }

    public TreeRecord mergePrefix(TreeRecord base, TreeRecord left, TreeRecord right, byte[] prefix, String resolver)
            throws ProllyBindingException {
        return engine.mergePrefix(base, left, right, prefix.clone(), resolver);
    }

    public TreeRecord mergePrefixWithResolver(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            byte[] prefix,
            MergeResolverCallback resolver) throws ProllyBindingException {
        return engine.mergePrefixWithResolver(base, left, right, prefix.clone(), resolver);
    }

    public TreeRecord mergePrefixWithPolicy(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            byte[] prefix,
            MergePolicyRegistry policy) throws ProllyBindingException {
        return engine.mergePrefixWithPolicy(base, left, right, prefix.clone(), policy);
    }

    public Optional<TreeRecord> loadNamedRoot(byte[] name) throws ProllyBindingException {
        return Optional.ofNullable(engine.loadNamedRoot(name.clone()));
    }

    public NamedRootSelectionRecord loadNamedRoots(List<byte[]> names) throws ProllyBindingException {
        return engine.loadNamedRoots(cloneByteArrays(names));
    }

    public NamedRootSelectionRecord loadRetainedNamedRoots(NamedRootRetentionRecord retention)
            throws ProllyBindingException {
        return engine.loadRetainedNamedRoots(retention);
    }

    public List<NamedRootRecord> listNamedRoots() throws ProllyBindingException {
        return engine.listNamedRoots();
    }

    public List<NamedRootManifest> listNamedRootManifests() throws ProllyBindingException {
        return namedRootManifests(engine.listNamedRootManifests());
    }

    public void publishNamedRoot(byte[] name, TreeRecord tree) throws ProllyBindingException {
        engine.publishNamedRoot(name.clone(), tree);
    }

    public void publishNamedRootAtMillis(byte[] name, TreeRecord tree, long timestampMillis)
            throws ProllyBindingException {
        ProllyJavaAdapters.publishNamedRootAtMillis(engine, name.clone(), tree, timestampMillis);
    }

    public void deleteNamedRoot(byte[] name) throws ProllyBindingException {
        engine.deleteNamedRoot(name.clone());
    }

    public NamedRootUpdateRecord compareAndSwapNamedRoot(
            byte[] name,
            Optional<TreeRecord> expected,
            Optional<TreeRecord> replacement) throws ProllyBindingException {
        return engine.compareAndSwapNamedRoot(name.clone(), expected.orElse(null), replacement.orElse(null));
    }

    public NamedRootUpdateRecord compareAndSwapNamedRootAtMillis(
            byte[] name,
            Optional<TreeRecord> expected,
            Optional<TreeRecord> replacement,
            long timestampMillis) throws ProllyBindingException {
        return ProllyJavaAdapters.compareAndSwapNamedRootAtMillis(
                engine,
                name.clone(),
                expected.orElse(null),
                replacement.orElse(null),
                timestampMillis);
    }

    public void publishSnapshot(SnapshotNamespaceRecord namespace, byte[] id, TreeRecord tree)
            throws ProllyBindingException {
        engine.publishSnapshot(cloneSnapshotNamespace(namespace), id.clone(), tree);
    }

    public void publishSnapshotAtMillis(
            SnapshotNamespaceRecord namespace,
            byte[] id,
            TreeRecord tree,
            long timestampMillis) throws ProllyBindingException {
        ProllyJavaAdapters.publishSnapshotAtMillis(
                engine,
                cloneSnapshotNamespace(namespace),
                id.clone(),
                tree,
                timestampMillis);
    }

    public Optional<TreeRecord> loadSnapshot(SnapshotNamespaceRecord namespace, byte[] id)
            throws ProllyBindingException {
        return Optional.ofNullable(engine.loadSnapshot(cloneSnapshotNamespace(namespace), id.clone()));
    }

    public SnapshotSelection loadSnapshots(SnapshotNamespaceRecord namespace, List<byte[]> ids)
            throws ProllyBindingException {
        SnapshotSelectionRecord selection =
                engine.loadSnapshots(cloneSnapshotNamespace(namespace), cloneByteArrays(ids));
        return new SnapshotSelection(
                snapshotRoots(selection.getSnapshots()),
                cloneByteArrays(selection.getMissingIds()));
    }

    public List<SnapshotRoot> listSnapshots(SnapshotNamespaceRecord namespace)
            throws ProllyBindingException {
        return snapshotRoots(engine.listSnapshots(cloneSnapshotNamespace(namespace)));
    }

    public void deleteSnapshot(SnapshotNamespaceRecord namespace, byte[] id)
            throws ProllyBindingException {
        engine.deleteSnapshot(cloneSnapshotNamespace(namespace), id.clone());
    }

    public NamedRootUpdateRecord compareAndSwapSnapshot(
            SnapshotNamespaceRecord namespace,
            byte[] id,
            Optional<TreeRecord> expected,
            Optional<TreeRecord> replacement) throws ProllyBindingException {
        return engine.compareAndSwapSnapshot(
                cloneSnapshotNamespace(namespace),
                id.clone(),
                expected.orElse(null),
                replacement.orElse(null));
    }

    public NamedRootUpdateRecord compareAndSwapSnapshotAtMillis(
            SnapshotNamespaceRecord namespace,
            byte[] id,
            Optional<TreeRecord> expected,
            Optional<TreeRecord> replacement,
            long timestampMillis) throws ProllyBindingException {
        return ProllyJavaAdapters.compareAndSwapSnapshotAtMillis(
                engine,
                cloneSnapshotNamespace(namespace),
                id.clone(),
                expected.orElse(null),
                replacement.orElse(null),
                timestampMillis);
    }

    public String collectStatsJson(TreeRecord tree) throws ProllyBindingException {
        return engine.collectStatsJson(tree).getJson();
    }

    public TreeStatsRecord collectStats(TreeRecord tree) throws ProllyBindingException {
        return engine.collectStats(tree);
    }

    public String statsDiffJson(TreeRecord before, TreeRecord after) throws ProllyBindingException {
        return engine.statsDiffJson(before, after).getJson();
    }

    public StatsComparisonRecord statsDiff(TreeRecord before, TreeRecord after) throws ProllyBindingException {
        return engine.statsDiff(before, after);
    }

    public String debugTreeJson(TreeRecord tree) throws ProllyBindingException {
        return engine.debugTreeJson(tree).getJson();
    }

    public TreeDebugViewRecord debugTree(TreeRecord tree) throws ProllyBindingException {
        return engine.debugTree(tree);
    }

    public String debugTreeText(TreeRecord tree) throws ProllyBindingException {
        return engine.debugTreeText(tree);
    }

    public String debugCompareTreesJson(TreeRecord left, TreeRecord right) throws ProllyBindingException {
        return engine.debugCompareTreesJson(left, right).getJson();
    }

    public TreeDebugComparisonRecord debugCompareTrees(TreeRecord left, TreeRecord right) throws ProllyBindingException {
        return engine.debugCompareTrees(left, right);
    }

    public String debugCompareTreesText(TreeRecord left, TreeRecord right) throws ProllyBindingException {
        return engine.debugCompareTreesText(left, right);
    }

    public CacheStats cacheStats() throws ProllyBindingException {
        return new CacheStats(engine.cacheStats());
    }

    public void clearCache() {
        engine.clearCache();
    }

    public long pinTreeRoot(TreeRecord tree) throws ProllyBindingException {
        return ProllyJavaAdapters.pinTreeRoot(engine, tree);
    }

    public long pinTreePath(TreeRecord tree, byte[] key) throws ProllyBindingException {
        return ProllyJavaAdapters.pinTreePath(engine, tree, key.clone());
    }

    public long unpinAllCacheNodes() throws ProllyBindingException {
        return ProllyJavaAdapters.unpinAllCacheNodes(engine);
    }

    public Metrics metrics() {
        return new Metrics(engine.metrics());
    }

    public void resetMetrics() {
        engine.resetMetrics();
    }

    public boolean publishPrefixPathHint(TreeRecord tree, byte[] prefix) throws ProllyBindingException {
        return engine.publishPrefixPathHint(tree, prefix.clone());
    }

    public boolean hydratePrefixPathHint(TreeRecord tree, byte[] prefix) throws ProllyBindingException {
        return engine.hydratePrefixPathHint(tree, prefix.clone());
    }

    public boolean publishChangedSpansHint(TreeRecord base, TreeRecord changed, List<ChangedSpanRecord> spans)
            throws ProllyBindingException {
        return engine.publishChangedSpansHint(base, changed, spans);
    }

    public ChangedSpanHintRecord loadChangedSpansHint(TreeRecord base, TreeRecord changed)
            throws ProllyBindingException {
        return engine.loadChangedSpansHint(base, changed);
    }

    public StructuralDiffPage structuralDiffPage(TreeRecord base, TreeRecord other, String cursorJson, long limit)
            throws ProllyBindingException {
        return new StructuralDiffPage(
                ProllyJavaAdapters.structuralDiffPage(engine, base, other, cursorJson, limit));
    }

    public StructuralDiffPage structuralDiffPageWithCursor(
            TreeRecord base,
            TreeRecord other,
            StructuralDiffCursorRecord cursor,
            long limit)
            throws ProllyBindingException {
        return new StructuralDiffPage(
                ProllyJavaAdapters.structuralDiffPageWithCursor(engine, base, other, cursor, limit));
    }

    public GcReachability markReachable(List<TreeRecord> roots) throws ProllyBindingException {
        return new GcReachability(engine.markReachable(roots));
    }

    public List<byte[]> listNodeCids() throws ProllyBindingException {
        return GcReachability.cloneByteArrays(engine.listNodeCids());
    }

    public GcPlan planGc(List<TreeRecord> roots, List<byte[]> candidateCids) throws ProllyBindingException {
        return new GcPlan(engine.planGc(roots, cloneByteArrays(candidateCids)));
    }

    public GcSweep sweepGc(List<TreeRecord> roots, List<byte[]> candidateCids) throws ProllyBindingException {
        return new GcSweep(engine.sweepGc(roots, cloneByteArrays(candidateCids)));
    }

    public GcPlan planStoreGc(List<TreeRecord> roots) throws ProllyBindingException {
        return new GcPlan(engine.planStoreGc(roots));
    }

    public GcSweep sweepStoreGc(List<TreeRecord> roots) throws ProllyBindingException {
        return new GcSweep(engine.sweepStoreGc(roots));
    }

    public GcPlan planStoreGcForRetention(NamedRootRetentionRecord retention) throws ProllyBindingException {
        return new GcPlan(engine.planStoreGcForRetention(retention));
    }

    public GcSweep sweepStoreGcForRetention(NamedRootRetentionRecord retention) throws ProllyBindingException {
        return new GcSweep(engine.sweepStoreGcForRetention(retention));
    }

    public BlobGcReachability markReachableBlobs(List<TreeRecord> roots) throws ProllyBindingException {
        return new BlobGcReachability(engine.markReachableBlobs(roots));
    }

    public BlobGcPlan planBlobGc(BlobStore blobStore, List<TreeRecord> roots, List<BlobRef> candidateBlobs)
            throws ProllyBindingException {
        return new BlobGcPlan(engine.planBlobGc(blobStore.inner(), roots, blobRefRecords(candidateBlobs)));
    }

    public BlobGcSweep sweepBlobGc(BlobStore blobStore, List<TreeRecord> roots, List<BlobRef> candidateBlobs)
            throws ProllyBindingException {
        return new BlobGcSweep(engine.sweepBlobGc(blobStore.inner(), roots, blobRefRecords(candidateBlobs)));
    }

    public BlobGcPlan planBlobStoreGc(BlobStore blobStore, List<TreeRecord> roots) throws ProllyBindingException {
        return new BlobGcPlan(engine.planBlobStoreGc(blobStore.inner(), roots));
    }

    public BlobGcSweep sweepBlobStoreGc(BlobStore blobStore, List<TreeRecord> roots) throws ProllyBindingException {
        return new BlobGcSweep(engine.sweepBlobStoreGc(blobStore.inner(), roots));
    }

    public MissingNodePlan planMissingNodes(TreeRecord tree, Prolly destination) throws ProllyBindingException {
        return new MissingNodePlan(engine.planMissingNodes(tree, destination.engine));
    }

    public MissingNodeCopy copyMissingNodes(TreeRecord tree, Prolly destination) throws ProllyBindingException {
        return new MissingNodeCopy(engine.copyMissingNodes(tree, destination.engine));
    }

    public SnapshotBundleRecord exportSnapshot(TreeRecord tree) throws ProllyBindingException {
        return engine.exportSnapshot(Objects.requireNonNull(tree));
    }

    public TreeRecord importSnapshot(SnapshotBundleRecord bundle) throws ProllyBindingException {
        return engine.importSnapshot(Objects.requireNonNull(bundle));
    }

    @Override
    public void close() {
        engine.close();
    }

    private static List<byte[]> cloneByteArrays(List<byte[]> values) {
        List<byte[]> cloned = new ArrayList<>(values.size());
        for (byte[] value : values) {
            cloned.add(value.clone());
        }
        return cloned;
    }

    private static List<MutationRecord> cloneMutations(List<MutationRecord> mutations) {
        List<MutationRecord> cloned = new ArrayList<>(mutations.size());
        for (MutationRecord mutation : mutations) {
            byte[] value = mutation.getValue();
            cloned.add(new MutationRecord(
                    mutation.getKind(),
                    mutation.getKey().clone(),
                    value == null ? null : value.clone()));
        }
        return cloned;
    }

    private static List<EntryRecord> entryRecords(List<Entry> entries) {
        List<EntryRecord> records = new ArrayList<>(entries.size());
        for (Entry entry : entries) {
            records.add(new EntryRecord(entry.key(), entry.value()));
        }
        return records;
    }

    private static Entry entry(EntryRecord record) {
        return new Entry(record.getKey(), record.getValue());
    }

    private static SnapshotNamespaceRecord cloneSnapshotNamespace(SnapshotNamespaceRecord namespace) {
        byte[] customPrefix = namespace.getCustomPrefix();
        return new SnapshotNamespaceRecord(
                namespace.getKind(),
                customPrefix == null ? null : customPrefix.clone());
    }

    private static List<SnapshotRoot> snapshotRoots(List<SnapshotRecord> records) {
        List<SnapshotRoot> roots = new ArrayList<>(records.size());
        for (SnapshotRecord record : records) {
            roots.add(snapshotRoot(record));
        }
        return List.copyOf(roots);
    }

    private static SnapshotRoot snapshotRoot(SnapshotRecord record) {
        return new SnapshotRoot(
                record.getId().clone(),
                record.getName().clone(),
                record.getTree(),
                ProllyJavaAdapters.snapshotCreatedAtMillis(record),
                ProllyJavaAdapters.snapshotUpdatedAtMillis(record));
    }

    private static List<NamedRootManifest> namedRootManifests(List<NamedRootManifestRecord> records) {
        List<NamedRootManifest> manifests = new ArrayList<>(records.size());
        for (NamedRootManifestRecord record : records) {
            manifests.add(namedRootManifest(record));
        }
        return List.copyOf(manifests);
    }

    private static NamedRootManifest namedRootManifest(NamedRootManifestRecord record) {
        RootManifestRecord manifest = record.getManifest();
        return new NamedRootManifest(
                record.getName().clone(),
                new RootManifest(
                        manifest.getTree(),
                        ProllyJavaAdapters.rootManifestCreatedAtMillis(manifest),
                        ProllyJavaAdapters.rootManifestUpdatedAtMillis(manifest)));
    }

    private static List<BlobRefRecord> blobRefRecords(List<BlobRef> refs) {
        List<BlobRefRecord> records = new ArrayList<>(refs.size());
        for (BlobRef ref : refs) {
            records.add(ref.toRecord());
        }
        return List.copyOf(records);
    }

    private static List<TombstoneMetadataRecord> cloneTombstoneMetadata(List<TombstoneMetadataRecord> metadata) {
        List<TombstoneMetadataRecord> cloned = new ArrayList<>(metadata.size());
        for (TombstoneMetadataRecord entry : metadata) {
            cloned.add(new TombstoneMetadataRecord(entry.getKey(), entry.getValue().clone()));
        }
        return List.copyOf(cloned);
    }
}
