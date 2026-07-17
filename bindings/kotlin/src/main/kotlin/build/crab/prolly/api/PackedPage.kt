package build.crab.prolly.api

import com.sun.jna.Library
import com.sun.jna.Memory
import com.sun.jna.Native
import com.sun.jna.Pointer
import com.sun.jna.Structure
import java.lang.ref.Cleaner
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.atomic.AtomicBoolean

internal interface FastAbi : Library {
    class ValueLeaseResult : Structure(), Structure.ByValue {
        @JvmField var status: Int = 0
        @JvmField var found: Byte = 0
        @JvmField var reserved: ByteArray = ByteArray(3)
        @JvmField var leaseHandle: Long = 0
        @JvmField var dataPtr: Pointer? = null
        @JvmField var dataLen: Long = 0

        override fun getFieldOrder() = listOf(
            "status", "found", "reserved", "leaseHandle", "dataPtr", "dataLen",
        )
    }

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
    fun prolly_fast_read_session_scan_open(
        sessionHandle: Long,
        start: Pointer?,
        startLen: Long,
        end: Pointer?,
        endLen: Long,
        hasEnd: Byte,
    ): ScanOpenResult
    fun prolly_fast_read_session_scan_next(
        sessionHandle: Long,
        scanHandle: Long,
        maxRecords: Int,
        maxArenaBytes: Long,
    ): PageResult
    fun prolly_fast_scan_close(scanHandle: Long)
    fun prolly_fast_page_release(leaseHandle: Long)
    fun prolly_fast_read_session_get_lease(
        sessionHandle: Long,
        key: Pointer?,
        keyLen: Long,
    ): ValueLeaseResult
    fun prolly_fast_proximity_get_lease(
        mapHandle: Long,
        key: Pointer?,
        keyLen: Long,
    ): ValueLeaseResult
    fun prolly_fast_value_release(leaseHandle: Long)

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

/** Read-only native page bytes that expire when their callback returns. */
class ScopedBytes internal constructor(
    private val source: ByteBuffer,
    private val offset: Int,
    val size: Int,
    private val scope: PageScope,
) {
    operator fun get(index: Int): Byte {
        scope.check()
        require(index in 0 until size) { "scoped byte index is out of bounds" }
        return source.get(offset + index)
    }

    fun bytes(): ByteArray {
        scope.check()
        val result = ByteArray(size)
        source.duplicate().position(offset).get(result)
        return result
    }

    internal fun subview(start: Int, length: Int): ScopedBytes {
        scope.check()
        require(start >= 0 && length >= 0 && start <= size && length <= size - start)
        return ScopedBytes(source, offset + start, length, scope)
    }

}

sealed interface ValueRefView {
    data class Inline(val value: ScopedBytes) : ValueRefView
    /** Blob length stored as unsigned bits; use toULong() in Kotlin. */
    data class Blob(val cid: ByteArray, val length: Long) : ValueRefView
}

/** One callback-scoped entry from a retained packed read scan. */
data class EntryView(val key: ScopedBytes, val value: ScopedBytes)

data class ReadScanOutcome(val visited: Long, val stopped: Boolean)

data class NeighborView(
    val key: ScopedBytes,
    val distance: Double,
    val rank: UInt,
    val value: ScopedBytes?,
    val proof: ScopedBytes?,
)

class ProximityVectorView internal constructor(
    private val pointer: Pointer,
    private val offset: Long,
    val dimensions: Int,
    private val scope: PageScope,
) {
    fun component(index: Int): Float {
        scope.check()
        require(index in 0 until dimensions) { "proximity vector index is out of range" }
        return java.lang.Float.intBitsToFloat(
            pointer.getByte(offset + index * 4L).toInt().and(0xff) or
                (pointer.getByte(offset + index * 4L + 1).toInt().and(0xff) shl 8) or
                (pointer.getByte(offset + index * 4L + 2).toInt().and(0xff) shl 16) or
                (pointer.getByte(offset + index * 4L + 3).toInt().and(0xff) shl 24),
        )
    }
    fun floats(): FloatArray = FloatArray(dimensions, ::component)
}

data class ProximityRecordView(val vector: ProximityVectorView, val value: ScopedBytes)

data class IndexMatchView(
    val term: ScopedBytes,
    val primaryKey: ScopedBytes,
    val projection: ScopedBytes?,
)

class PackedIndexPage internal constructor(
    leaseHandle: Long,
    scope: PageScope,
    val rows: List<IndexMatchView>,
) : AutoCloseable {
    private class Lease(private val leaseHandle: Long, private val scope: PageScope) : Runnable {
        private val closed = AtomicBoolean(false)
        override fun run() {
            if (closed.compareAndSet(false, true)) {
                scope.close()
                FastAbi.INSTANCE.prolly_fast_page_release(leaseHandle)
            }
        }
    }

    private val cleanable = cleaner.register(this, Lease(leaseHandle, scope))

    override fun close() = cleanable.clean()

    companion object {
        private val cleaner: Cleaner = Cleaner.create()
    }
}

internal object PackedPages {
    private const val HEADER_SIZE = 28
    private const val MAX_ARENA = 64L * 1024 * 1024
    private const val READ_SCAN_ARENA = 4L * 1024 * 1024
    private const val READ_SCAN_RECORDS = 4096

    fun withPointValue(
        sessionHandle: ULong,
        key: ByteArray,
        block: (ScopedBytes) -> Unit,
    ): Boolean {
        val keyMemory = Memory(key.size.toLong().coerceAtLeast(1))
        if (key.isNotEmpty()) keyMemory.write(0, key, 0, key.size)
        val result = FastAbi.INSTANCE.prolly_fast_read_session_get_lease(
            sessionHandle.toLong(), if (key.isEmpty()) null else keyMemory, key.size.toLong(),
        )
        check(result.status == 0) {
            "native retained point read failed with status ${result.status}"
        }
        if (result.found.toInt() == 0) {
            check(result.leaseHandle == 0L) { "missing point read returned a value lease" }
            return false
        }
        check(result.leaseHandle != 0L && result.dataLen >= 0 &&
            (result.dataLen == 0L || result.dataPtr != null)) {
            "native point read returned an invalid value lease"
        }
        val scope = PageScope()
        try {
            val buffer = if (result.dataLen == 0L) {
                ByteBuffer.allocate(0)
            } else {
                result.dataPtr!!.getByteBuffer(0, result.dataLen)
            }
            block(ScopedBytes(buffer, 0, result.dataLen.toInt(), scope))
            return true
        } finally {
            scope.close()
            FastAbi.INSTANCE.prolly_fast_value_release(result.leaseHandle)
        }
    }

    fun withProximityRecord(
        mapHandle: ULong,
        key: ByteArray,
        block: (ProximityRecordView) -> Unit,
    ): Boolean {
        val keyMemory = Memory(key.size.toLong().coerceAtLeast(1))
        if (key.isNotEmpty()) keyMemory.write(0, key, 0, key.size)
        val result = FastAbi.INSTANCE.prolly_fast_proximity_get_lease(
            mapHandle.toLong(), if (key.isEmpty()) null else keyMemory, key.size.toLong(),
        )
        check(result.status == 0) { "native retained proximity read failed with status ${result.status}" }
        if (result.found.toInt() == 0) {
            check(result.leaseHandle == 0L) { "missing proximity read returned a value lease" }
            return false
        }
        check(result.leaseHandle != 0L && result.dataLen >= 8 && result.dataPtr != null) {
            "native proximity read returned an invalid value lease"
        }
        val pointer = result.dataPtr!!
        val header = pointer.getByteArray(0, 6)
        val expected = byteArrayOf(0x50, 0x52, 0x56, 0x52, 2, 1)
        val scope = PageScope()
        try {
            require(header.contentEquals(expected)) { "invalid retained proximity record header" }
            val (dimensions, vectorStart) = readVarint(pointer, 6, result.dataLen)
            require(dimensions <= Int.MAX_VALUE.toLong()) { "proximity dimensions exceed the JVM limit" }
            val vectorBytes = dimensions * 4
            require(vectorStart + vectorBytes <= result.dataLen) { "retained proximity vector is truncated" }
            val (valueLength, valueStart) = readVarint(pointer, vectorStart + vectorBytes, result.dataLen)
            require(valueStart + valueLength == result.dataLen) { "retained proximity value length is invalid" }
            val bytes = pointer.getByteBuffer(0, result.dataLen)
            block(ProximityRecordView(
                ProximityVectorView(pointer, vectorStart, dimensions.toInt(), scope),
                ScopedBytes(bytes, valueStart.toInt(), valueLength.toInt(), scope),
            ))
            return true
        } finally {
            scope.close()
            FastAbi.INSTANCE.prolly_fast_value_release(result.leaseHandle)
        }
    }

    private fun readVarint(pointer: Pointer, start: Long, length: Long): Pair<Long, Long> {
        var offset = start
        var shift = 0
        var value = 0L
        while (offset < length && shift < 64) {
            val byte = pointer.getByte(offset).toInt().and(0xff)
            offset += 1
            value = value or ((byte and 0x7f).toLong() shl shift)
            if (byte and 0x80 == 0) return value to offset
            shift += 7
        }
        error("invalid proximity record varint")
    }

    fun withValueRef(
        sessionHandle: ULong,
        key: ByteArray,
        block: (ValueRefView) -> Unit,
    ): Boolean = withPointValue(sessionHandle, key) { value ->
        block(decodeValueRef(value))
    }

    private fun decodeValueRef(value: ScopedBytes): ValueRefView {
        val magic = byteArrayOf(0x50, 0x4c, 0x56, 0x42)
        if (value.size < 4 || magic.indices.any { value[it] != magic[it] }) {
            return ValueRefView.Inline(value)
        }
        require(value.size >= 6 && value[4].toInt() == 1) {
            "invalid or unsupported value reference header"
        }
        return when (value[5].toInt()) {
            0 -> {
                require(value.size >= 14) { "inline value reference is truncated" }
                val length = readBigEndianULong(value, 6)
                require(length <= Int.MAX_VALUE.toULong() && value.size == 14 + length.toInt()) {
                    "inline value reference length does not match payload"
                }
                ValueRefView.Inline(value.subview(14, length.toInt()))
            }
            1 -> {
                require(value.size == 46) { "blob value reference length is invalid" }
                ValueRefView.Blob(
                    value.subview(6, 32).bytes(), readBigEndianULong(value, 38).toLong(),
                )
            }
            else -> error("unknown value reference tag ${value[5]}")
        }
    }

    private fun readBigEndianULong(value: ScopedBytes, start: Int): ULong {
        var result = 0uL
        repeat(8) { index ->
            result = (result shl 8) or value[start + index].toUByte().toULong()
        }
        return result
    }

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
                    val base = HEADER_SIZE + index * 36
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

    fun openIndexExact(
        snapshotHandle: ULong,
        term: ByteArray,
        limit: UInt,
    ): PackedIndexPage {
        require(limit > 0u) { "limit must be positive" }
        val termMemory = Memory(term.size.toLong().coerceAtLeast(1))
        if (term.isNotEmpty()) termMemory.write(0, term, 0, term.size)
        val opened = FastAbi.INSTANCE.prolly_fast_index_cursor_open(
            snapshotHandle.toLong(), 1, termMemory, term.size.toLong(), null, 0, 0, 0,
        )
        check(opened.status == 0) { "native index cursor open failed with status ${opened.status}" }
        val result = try {
            FastAbi.INSTANCE.prolly_fast_index_cursor_next(
                snapshotHandle.toLong(), opened.scanHandle, limit.toInt(), MAX_ARENA,
            )
        } finally {
            FastAbi.INSTANCE.prolly_fast_index_cursor_close(opened.scanHandle)
        }
        check(result.status == 0) { "native index cursor read failed with status ${result.status}" }
        val scope = PageScope()
        try {
            val pointer = requireNotNull(result.dataPtr) { "native page pointer was null" }
            val length = Math.toIntExact(result.dataLen)
            val page = pointer.getByteBuffer(0, result.dataLen).order(ByteOrder.LITTLE_ENDIAN)
            require(page.get(0) == 'P'.code.toByte() && page.get(1) == 'R'.code.toByte() &&
                page.get(2) == 'P'.code.toByte() && page.get(3) == 'G'.code.toByte()) { "invalid packed page magic" }
            require(page.getShort(4).toInt() == 2 && page.getShort(6).toInt() == 5) { "unexpected packed page kind" }
            val count = page.getInt(12)
            val tableBytes = page.getInt(16)
            val arenaBytes = page.getLong(20)
            require(tableBytes == count * 36 && HEADER_SIZE + tableBytes + arenaBytes == length.toLong()) {
                "invalid packed page bounds"
            }
            val arenaStart = HEADER_SIZE + tableBytes
            val rows = ArrayList<IndexMatchView>(count)
            repeat(count) { index ->
                val base = HEADER_SIZE + index * 36
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
            return PackedIndexPage(result.leaseHandle, scope, rows)
        } catch (error: Throwable) {
            scope.close()
            FastAbi.INSTANCE.prolly_fast_page_release(result.leaseHandle)
            throw error
        }
    }

    fun scanRangeView(
        sessionHandle: ULong,
        start: ByteArray,
        end: ByteArray?,
        block: (EntryView) -> Boolean,
    ): ReadScanOutcome {
        val startMemory = inputMemory(start)
        val endMemory = end?.let(::inputMemory)
        val opened = FastAbi.INSTANCE.prolly_fast_read_session_scan_open(
            sessionHandle.toLong(), startMemory, start.size.toLong(), endMemory,
            end?.size?.toLong() ?: 0, if (end == null) 0 else 1,
        )
        check(opened.status == 0) { "native retained scan open failed with status ${opened.status}" }
        var visited = 0L
        var previousPageKey: ByteArray? = null
        try {
            while (true) {
                val result = FastAbi.INSTANCE.prolly_fast_read_session_scan_next(
                    sessionHandle.toLong(), opened.scanHandle, READ_SCAN_RECORDS, READ_SCAN_ARENA,
                )
                check(result.status == 0) { "native retained scan read failed with status ${result.status}" }
                var stopped = false
                var lastKey: ByteArray? = null
                withPage(
                    result,
                    expectedVersion = 1,
                    expectedKind = 1,
                    recordWidth = 16,
                    expectedTerminal = result.terminal.toInt() != 0,
                ) { page, arenaStart, count, scope ->
                    require(count == result.recordCount) { "packed scan record count mismatch" }
                    val arenaBytes = page.capacity() - arenaStart
                    var previousView: ScopedBytes? = null
                    for (index in 0 until count) {
                        val base = HEADER_SIZE + index * 16
                        val keyOffset = page.getInt(base)
                        val keyLength = page.getInt(base + 4)
                        val valueOffset = page.getInt(base + 8)
                        val valueLength = page.getInt(base + 12)
                        requirePackedRange(keyOffset, keyLength, arenaBytes, "scan key")
                        requirePackedRange(valueOffset, valueLength, arenaBytes, "scan value")
                        val key = ScopedBytes(page, arenaStart + keyOffset, keyLength, scope)
                        val value = ScopedBytes(page, arenaStart + valueOffset, valueLength, scope)
                        val ordered = when {
                            previousView != null -> compareUnsigned(previousView, key) < 0
                            previousPageKey != null -> compareUnsigned(previousPageKey!!, key) < 0
                            else -> true
                        }
                        require(ordered) { "packed scan page keys are not strictly ordered" }
                        previousView = key
                        visited += 1
                        if (!block(EntryView(key, value))) {
                            stopped = true
                            break
                        }
                    }
                    if (count > 0 && !stopped) lastKey = previousView!!.bytes()
                }
                if (stopped) return ReadScanOutcome(visited, true)
                if (result.terminal.toInt() != 0) return ReadScanOutcome(visited, false)
                check(lastKey != null) { "non-terminal packed scan page made no progress" }
                previousPageKey = lastKey
            }
        } finally {
            FastAbi.INSTANCE.prolly_fast_scan_close(opened.scanHandle)
        }
    }

    private fun <R> withPage(
        result: FastAbi.PageResult,
        expectedVersion: Int = 2,
        expectedKind: Int,
        recordWidth: Int,
        expectedTerminal: Boolean? = null,
        block: (ByteBuffer, Int, Int, PageScope) -> R,
    ): R {
        val pointer = requireNotNull(result.dataPtr) { "native page pointer was null" }
        val length = Math.toIntExact(result.dataLen)
        val page = pointer.getByteBuffer(0, result.dataLen).order(ByteOrder.LITTLE_ENDIAN)
        val scope = PageScope()
        try {
            require(page.get(0) == 'P'.code.toByte() && page.get(1) == 'R'.code.toByte() &&
                page.get(2) == 'P'.code.toByte() && page.get(3) == 'G'.code.toByte()) { "invalid packed page magic" }
            require(page.getShort(4).toInt() == expectedVersion && page.getShort(6).toInt() == expectedKind) { "unexpected packed page kind" }
            expectedTerminal?.let { terminal ->
                require((page.getInt(8) and 1 != 0) == terminal) { "packed page terminal flag mismatch" }
            }
            val count = page.getInt(12)
            val tableBytes = page.getInt(16)
            val arenaBytes = page.getLong(20)
            require(
                tableBytes >= count * recordWidth && tableBytes % recordWidth == 0 &&
                    HEADER_SIZE + tableBytes + arenaBytes == length.toLong()
            ) {
                "invalid packed page bounds"
            }
            return block(page, HEADER_SIZE + tableBytes, count, scope)
        } finally {
            scope.close()
            FastAbi.INSTANCE.prolly_fast_page_release(result.leaseHandle)
        }
    }

    private fun inputMemory(bytes: ByteArray): Memory? {
        if (bytes.isEmpty()) return null
        return Memory(bytes.size.toLong()).also { it.write(0, bytes, 0, bytes.size) }
    }

    private fun requirePackedRange(offset: Int, length: Int, arenaBytes: Int, field: String) {
        require(offset >= 0 && length >= 0 && offset <= arenaBytes - length) {
            "$field is outside the packed page arena"
        }
    }

    private fun compareUnsigned(left: ScopedBytes, right: ScopedBytes): Int {
        val shared = minOf(left.size, right.size)
        repeat(shared) { index ->
            val comparison = (left[index].toInt() and 0xff).compareTo(right[index].toInt() and 0xff)
            if (comparison != 0) return comparison
        }
        return left.size.compareTo(right.size)
    }

    private fun compareUnsigned(left: ByteArray, right: ScopedBytes): Int {
        val shared = minOf(left.size, right.size)
        repeat(shared) { index ->
            val comparison = (left[index].toInt() and 0xff).compareTo(right[index].toInt() and 0xff)
            if (comparison != 0) return comparison
        }
        return left.size.compareTo(right.size)
    }
}
