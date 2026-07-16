package build.crab.prolly;

@FunctionalInterface
public interface ConflictScanVisitor {
    /** Return true to continue or false to stop after this conflict. */
    boolean visit(ConflictRecord conflict);
}
