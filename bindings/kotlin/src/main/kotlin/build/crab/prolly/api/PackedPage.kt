package build.crab.prolly.api

import com.sun.jna.Library
import com.sun.jna.Memory
import com.sun.jna.Native
import com.sun.jna.Pointer
import com.sun.jna.Structure
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.atomic.AtomicBoolean

internal interface FastAbi : Library {
    class PageResult : Structure(), Structure.ByValue {
        @JvmField var status: Int = 0
        @JvmField var terminal: Byte = 0
        @JvmField var reserved: ByteArray = ByteArray(3)
        @JvmField var recordCount: Int = 0
        @JvmField var leaseHandle: Long = 0
        @JvmField var dataPtr: Pointer? = null
        @JvmField var dataLen: Long = 0

        override fun getFieldOrder() = listOf(
            "status", "terminal", "reserved", "recordCount", "leaseHandle", "dataPtr", "dataLen",
        )
    }

    fun prolly_fast_proximity_search(
        mapHandle: Long,
        query: Pointer,
        dimensions: Long,
        k: Int,
        maxArenaBytes: Long,
    ): PageResult

    fun prolly_fast_index_cursor_open(
        snapshotHandle: Long,
        queryKind: Int,
        start: Pointer?,
        startLen: Long,
        end: Pointer?,
        endLen: Long,
        hasEnd: Byte,
        reverse: Byte,
    ): ScanOpenResult

    fun prolly_fast_index_cursor_next(
        snapshotHandle: Long,
        cursorHandle: Long,
        limit: Int,
        maxArenaBytes: Long,
    ): PageResult
    fun prolly_fast_index_cursor_close(cursorHandle: Long)
    fun prolly_fast_page_release(leaseHandle: Long)

    class ScanOpenResult : Structure(), Structure.ByValue {
        @JvmField var status: Int = 0
        @JvmField var reserved: Int = 0
        @JvmField var scanHandle: Long = 0
        override fun getFieldOrder() = listOf("status", "reserved", "scanHandle")
    }

    companion object {
        val INSTANCE: FastAbi by lazy {
            val library = System.getProperty("uniffi.component.prolly.libraryOverride")
                ?: "prolly_bindings"
            Native.load(library, FastAbi::class.java)
        }
    }
}

internal class PageScope {
    private val alive = AtomicBoolean(true)
    fun close() { alive.set(false) }
    fun check() = check(alive.get()) { "packed page view escaped its callback scope" }
}

class ScopedBytes internal constructor(
    private val source: ByteBuffer,
    private val offset: Int,
    val size: Int,
    private val scope: PageScope,
) {
    fun bytes(): ByteArray {
        scope.check()
        val result = ByteArray(size)
        source.duplicate().position(offset).get(result)
        return result
    }

    fun buffer(): ByteBuffer {
        scope.check()
        return source.duplicate().position(offset).limit(offset + size).slice().asReadOnlyBuffer()
    }
}

data class NeighborView(
    val key: ScopedBytes,
    val distance: Double,
    val rank: UInt,
    val value: ScopedBytes?,
    val proof: ScopedBytes?,
)

data class IndexMatchView(
    val term: ScopedBytes,
    val primaryKey: ScopedBytes,
    val projection: ScopedBytes?,
)

internal object PackedPages {
    private const val HEADER_SIZE = 28
    private const val MAX_ARENA = 64L * 1024 * 1024

    fun <R> withProximitySearch(
        mapHandle: ULong,
        query: List<Float>,
        k: UInt,
        block: (List<NeighborView>) -> R,
    ): R {
        require(query.isNotEmpty()) { "query must not be empty" }
        val queryMemory = Memory(query.size.toLong() * Float.SIZE_BYTES)
        query.forEachIndexed { index, value -> queryMemory.setFloat(index.toLong() * Float.SIZE_BYTES, value) }
        val result = FastAbi.INSTANCE.prolly_fast_proximity_search(
            mapHandle.toLong(), queryMemory, query.size.toLong(), k.toInt(), MAX_ARENA,
        )
        check(result.status == 0) { "native proximity search failed with status ${result.status}" }
        return withPage(result, expectedKind = 7, recordWidth = 40) { page, arenaStart, count, scope ->
            val rows = ArrayList<NeighborView>(count)
            repeat(count) { index ->
                val base = HEADER_SIZE + index * 40
                val flags = page.getInt(base)
                val keyOffset = page.getInt(base + 4)
                val keyLength = page.getInt(base + 8)
                val distance = page.getDouble(base + 12)
                val rank = page.getInt(base + 20).toUInt()
                val valueOffset = page.getInt(base + 24)
                val valueLength = page.getInt(base + 28)
                val proofOffset = page.getInt(base + 32)
                val proofLength = page.getInt(base + 36)
                rows += NeighborView(
                    ScopedBytes(page, arenaStart + keyOffset, keyLength, scope), distance, rank,
                    if (flags and 1 != 0) ScopedBytes(page, arenaStart + valueOffset, valueLength, scope) else null,
                    if (flags and 2 != 0) ScopedBytes(page, arenaStart + proofOffset, proofLength, scope) else null,
                )
            }
            block(rows)
        }
    }

    fun <R> withIndexExact(
        snapshotHandle: ULong,
        term: ByteArray,
        limit: UInt,
        block: (List<IndexMatchView>) -> R,
    ): R {
        val termMemory = Memory(term.size.toLong().coerceAtLeast(1))
        if (term.isNotEmpty()) termMemory.write(0, term, 0, term.size)
        val opened = FastAbi.INSTANCE.prolly_fast_index_cursor_open(
            snapshotHandle.toLong(), 1, termMemory, term.size.toLong(), null, 0, 0, 0,
        )
        check(opened.status == 0) { "native index cursor open failed with status ${opened.status}" }
        try {
            val result = FastAbi.INSTANCE.prolly_fast_index_cursor_next(
                snapshotHandle.toLong(), opened.scanHandle, limit.toInt(), MAX_ARENA,
            )
            check(result.status == 0) { "native index cursor read failed with status ${result.status}" }
            return withPage(result, expectedKind = 5, recordWidth = 36) { page, arenaStart, count, scope ->
                val rows = ArrayList<IndexMatchView>(count)
                repeat(count) { index ->
                    val base = HEADER_SIZE + index * 28
                    val flags = page.getInt(base)
                    val termOffset = page.getInt(base + 4)
                    val termLength = page.getInt(base + 8)
                    val keyOffset = page.getInt(base + 12)
                    val keyLength = page.getInt(base + 16)
                    val projectionOffset = page.getInt(base + 20)
                    val projectionLength = page.getInt(base + 24)
                    rows += IndexMatchView(
                        ScopedBytes(page, arenaStart + termOffset, termLength, scope),
                        ScopedBytes(page, arenaStart + keyOffset, keyLength, scope),
                        if (flags and 1 != 0) ScopedBytes(page, arenaStart + projectionOffset, projectionLength, scope) else null,
                    )
                }
                block(rows)
            }
        } finally {
            FastAbi.INSTANCE.prolly_fast_index_cursor_close(opened.scanHandle)
        }
    }

    private fun <R> withPage(
        result: FastAbi.PageResult,
        expectedKind: Int,
        recordWidth: Int,
        block: (ByteBuffer, Int, Int, PageScope) -> R,
    ): R {
        val pointer = requireNotNull(result.dataPtr) { "native page pointer was null" }
        val length = Math.toIntExact(result.dataLen)
        val page = pointer.getByteBuffer(0, result.dataLen).order(ByteOrder.LITTLE_ENDIAN)
        val scope = PageScope()
        try {
            require(page.get(0) == 'P'.code.toByte() && page.get(1) == 'R'.code.toByte() &&
                page.get(2) == 'P'.code.toByte() && page.get(3) == 'G'.code.toByte()) { "invalid packed page magic" }
            require(page.getShort(4).toInt() == 2 && page.getShort(6).toInt() == expectedKind) { "unexpected packed page kind" }
            val count = page.getInt(12)
            val tableBytes = page.getInt(16)
            val arenaBytes = page.getLong(20)
            require(tableBytes == count * recordWidth && HEADER_SIZE + tableBytes + arenaBytes == length.toLong()) {
                "invalid packed page bounds"
            }
            return block(page, HEADER_SIZE + tableBytes, count, scope)
        } finally {
            scope.close()
            FastAbi.INSTANCE.prolly_fast_page_release(result.leaseHandle)
        }
    }
}
