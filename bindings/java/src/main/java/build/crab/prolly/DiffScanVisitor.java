package build.crab.prolly;

@FunctionalInterface
public interface DiffScanVisitor {
    /** Return true to continue or false to stop after this diff. */
    boolean visit(DiffRecord diff);
}
