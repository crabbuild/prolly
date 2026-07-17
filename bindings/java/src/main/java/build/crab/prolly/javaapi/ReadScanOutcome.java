package build.crab.prolly.javaapi;

/** Result of a retained packed range scan. */
public record ReadScanOutcome(long visited, boolean stopped) {}
