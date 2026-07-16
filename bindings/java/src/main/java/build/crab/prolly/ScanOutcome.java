package build.crab.prolly;

/** Result of a callback scan. The stopping record is included in visited. */
public record ScanOutcome(long visited, boolean stopped) {}
