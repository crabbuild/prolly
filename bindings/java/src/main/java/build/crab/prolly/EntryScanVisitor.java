package build.crab.prolly;

@FunctionalInterface
public interface EntryScanVisitor {
    /** Return true to continue or false to stop after this entry. */
    boolean visit(Entry entry);
}
