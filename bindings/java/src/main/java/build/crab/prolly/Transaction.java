package build.crab.prolly;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class Transaction implements AutoCloseable {
    private final ProllyTransaction inner;

    Transaction(ProllyTransaction inner) {
        this.inner = inner;
    }

    public TreeRecord create() throws ProllyBindingException {
        return inner.create();
    }

    public Optional<byte[]> get(TreeRecord tree, byte[] key) throws ProllyBindingException {
        return Optional.ofNullable(inner.get(tree, key.clone())).map(byte[]::clone);
    }

    public TreeRecord put(TreeRecord tree, byte[] key, byte[] value) throws ProllyBindingException {
        return inner.put(tree, key.clone(), value.clone());
    }

    public TreeRecord delete(TreeRecord tree, byte[] key) throws ProllyBindingException {
        return inner.delete(tree, key.clone());
    }

    public TreeRecord batch(TreeRecord tree, List<MutationRecord> mutations) throws ProllyBindingException {
        return inner.batch(tree, cloneMutations(mutations));
    }

    public Optional<TreeRecord> loadNamedRoot(byte[] name) throws ProllyBindingException {
        return Optional.ofNullable(inner.loadNamedRoot(name.clone()));
    }

    public void publishNamedRoot(byte[] name, TreeRecord tree) throws ProllyBindingException {
        inner.publishNamedRoot(name.clone(), tree);
    }

    public void deleteNamedRoot(byte[] name) throws ProllyBindingException {
        inner.deleteNamedRoot(name.clone());
    }

    public NamedRootUpdateRecord compareAndSwapNamedRoot(
            byte[] name,
            Optional<TreeRecord> expected,
            Optional<TreeRecord> replacement) throws ProllyBindingException {
        return inner.compareAndSwapNamedRoot(name.clone(), expected.orElse(null), replacement.orElse(null));
    }

    public TransactionUpdate commit() throws ProllyBindingException {
        return new TransactionUpdate(inner.commit());
    }

    public void rollback() throws ProllyBindingException {
        inner.rollback();
    }

    @Override
    public void close() {
        inner.close();
    }

    private static List<MutationRecord> cloneMutations(List<MutationRecord> mutations) {
        List<MutationRecord> cloned = new ArrayList<>(mutations.size());
        for (MutationRecord mutation : mutations) {
            cloned.add(new MutationRecord(
                    mutation.getKind(),
                    mutation.getKey().clone(),
                    mutation.getValue() == null ? null : mutation.getValue().clone()));
        }
        return cloned;
    }
}
