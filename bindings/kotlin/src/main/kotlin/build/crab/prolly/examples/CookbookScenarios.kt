package build.crab.prolly.examples

import java.nio.file.Files
import java.nio.file.Path

private val scenarios = listOf(
    "BasicMapKt",
    "DiffMergeKt",
    "FileBlobStoreKt",
    "SecondaryIndexKt",
    "BatchBuildKt",
    "LocalFirstStateKt",
    "ResolverKt",
    "CrdtMergeKt",
    "ConversationMemoryKt",
    "AgentEventLogKt",
    "BackgroundCompactionKt",
    "DeterministicRagSnapshotKt",
    "DocumentChunkIndexKt",
    "VectorSidecarKt",
    "ProvenanceValuesKt",
    "MaterializedViewKt",
    "FilesystemSnapshotKt",
    "DurableSqliteKt",
)

fun main() {
    val pom = modulePom()
    for (scenario in scenarios) {
        val process = ProcessBuilder(
            "mvn",
            "-q",
            "-f",
            pom.toString(),
            "-Dexec.mainClass=build.crab.prolly.examples.$scenario",
            "exec:java",
        )
            .inheritIO()
            .start()
        val exitCode = process.waitFor()
        require(exitCode == 0) { "$scenario failed with exit code $exitCode" }
    }
}

private fun modulePom(): Path {
    val location = Path.of(object {}.javaClass.protectionDomain.codeSource.location.toURI())
    val moduleDir =
        if (location.fileName.toString() == "classes") {
            location.parent.parent
        } else {
            location.parent
        }
    val pom = moduleDir.resolve("pom.xml")
    return if (Files.exists(pom)) pom else Path.of("pom.xml")
}
