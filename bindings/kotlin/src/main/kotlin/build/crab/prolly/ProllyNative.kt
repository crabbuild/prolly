package build.crab.prolly

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

object ProllyNative {
    private const val OVERRIDE_PROPERTY = "uniffi.component.prolly.libraryOverride"

    @JvmStatic
    fun useLibrary(path: String) {
        System.setProperty(OVERRIDE_PROPERTY, path)
    }

    @JvmStatic
    fun useLibrary(path: Path) {
        useLibrary(path.toAbsolutePath().normalize().toString())
    }

    @JvmStatic
    fun useLocalDebugLibrary(): Path {
        System.getProperty(OVERRIDE_PROPERTY)?.let { return Paths.get(it) }

        val libraryName = when {
            System.getProperty("os.name").lowercase().contains("win") -> "prolly_bindings.dll"
            System.getProperty("os.name").lowercase().contains("mac") -> "libprolly_bindings.dylib"
            else -> "libprolly_bindings.so"
        }
        val cwd = Paths.get("").toAbsolutePath().normalize()
        val candidates = listOf(
            cwd.resolve("../../../../target/debug/$libraryName"),
            cwd.resolve("../../../target/debug/$libraryName"),
            cwd.resolve("target/debug/$libraryName"),
        ).map { it.normalize() }

        val path = candidates.firstOrNull { Files.exists(it) }
            ?: error("Could not find $libraryName. Run `cargo build -p prolly-bindings` first.")
        useLibrary(path)
        return path
    }
}
