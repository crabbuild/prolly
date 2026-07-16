package build.crab.prolly;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A root-bound read session. Reuse one session for repeated reads so the
 * native engine does not decode and reacquire the same tree on every call.
 * Returned byte arrays and records are owned Java values.
 */
public final class ReadSession implements AutoCloseable {
    private final ProllyReadSession inner;
    private final AtomicBoolean closed = new AtomicBoolean();

    ReadSession(ProllyReadSession inner) {
        this.inner = Objects.requireNonNull(inner);
    }

    public Optional<byte[]> get(byte[] key) throws ProllyBindingException {
        ensureOpen();
        return Optional.ofNullable(inner.get(key.clone())).map(byte[]::clone);
    }

    public List<byte[]> getMany(List<byte[]> keys) throws ProllyBindingException {
        ensureOpen();
        List<byte[]> inputs = new ArrayList<>(keys.size());
        for (byte[] key : keys) {
            inputs.add(Objects.requireNonNull(key).clone());
        }
        List<byte[]> values = inner.getMany(inputs);
        List<byte[]> owned = new ArrayList<>(values.size());
        for (byte[] value : values) {
            owned.add(value == null ? null : value.clone());
        }
        return owned;
    }

    public ScanOutcome scanRange(
            byte[] start,
            Optional<byte[]> end,
            EntryScanVisitor visitor) throws ProllyBindingException {
        ensureOpen();
        Objects.requireNonNull(visitor);
        return outcome(inner.scanRange(
                start.clone(),
                end.map(byte[]::clone).orElse(null),
                record -> visitor.visit(new Entry(record.getKey(), record.getValue()))));
    }

    public ScanOutcome scanRangeDiff(
            ReadSession other,
            byte[] start,
            Optional<byte[]> end,
            DiffScanVisitor visitor) throws ProllyBindingException {
        ensureOpen();
        other.ensureOpen();
        Objects.requireNonNull(visitor);
        return outcome(inner.scanRangeDiff(
                other.inner,
                start.clone(),
                end.map(byte[]::clone).orElse(null),
                record -> visitor.visit(record)));
    }

    /** The receiver is the merge base. */
    public ScanOutcome scanConflicts(
            ReadSession left,
            ReadSession right,
            ConflictScanVisitor visitor) throws ProllyBindingException {
        ensureOpen();
        left.ensureOpen();
        right.ensureOpen();
        Objects.requireNonNull(visitor);
        return outcome(inner.scanConflicts(
                left.inner,
                right.inner,
                record -> visitor.visit(record)));
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("prolly read session is closed");
        }
    }

    private static ScanOutcome outcome(ScanOutcomeRecord record) {
        return new ScanOutcome(ProllyJavaAdapters.scanOutcomeVisited(record), record.getStopped());
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            inner.close();
        }
    }
}
