package build.crab.prolly.javaapi;

public record TypedMigrationResult(
        MapUpdate update,
        int scannedValues,
        int rewrittenValues) {}
