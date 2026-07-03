package build.crab.prolly.examples;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class CookbookScenarios {
    private static final List<String> SCENARIOS = List.of(
            "BasicMap",
            "DiffMerge",
            "FileBlobStore",
            "SecondaryIndex",
            "AgentEventLog",
            "BackgroundCompaction",
            "BatchBuild",
            "ConversationMemory",
            "CrdtMerge",
            "DeterministicRagSnapshot",
            "DocumentChunkIndex",
            "DurableSqlite",
            "FilesystemSnapshot",
            "LocalFirstState",
            "MaterializedView",
            "ProvenanceValues",
            "Resolver",
            "VectorSidecar");

    private CookbookScenarios() {
    }

    public static void main(String[] args) throws Exception {
        String pom = modulePom().toString();
        for (String scenario : SCENARIOS) {
            Process process = new ProcessBuilder(
                            "mvn",
                            "-q",
                            "-f",
                            pom,
                            "-Dexec.mainClass=build.crab.prolly.examples." + scenario,
                            "exec:java")
                    .inheritIO()
                    .start();
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IllegalStateException(scenario + " failed with exit code " + exitCode);
            }
        }
    }

    private static Path modulePom() throws Exception {
        Path location = Path.of(CookbookScenarios.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        Path moduleDir = location.getFileName().toString().equals("classes")
                ? location.getParent().getParent()
                : location.getParent();
        Path pom = moduleDir.resolve("pom.xml");
        return Files.exists(pom) ? pom : Path.of("pom.xml");
    }
}
