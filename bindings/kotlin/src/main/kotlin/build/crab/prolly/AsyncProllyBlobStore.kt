package build.crab.prolly

class AsyncProllyBlobStore private constructor(
    internal val store: ProllyBlobStore,
) : AutoCloseable {
    companion object {
        suspend fun memory(): AsyncProllyBlobStore = AsyncProllyBlobStore(ProllyBlobStore.memory())

        suspend fun file(path: String): AsyncProllyBlobStore = AsyncProllyBlobStore(ProllyBlobStore.file(path))

        fun wrap(store: ProllyBlobStore): AsyncProllyBlobStore = AsyncProllyBlobStore(store)
    }

    suspend fun putBlob(bytes: ByteArray): BlobRefRecord = store.putBlob(bytes)

    suspend fun getBlob(reference: BlobRefRecord): ByteArray? = store.getBlob(reference)

    suspend fun deleteBlob(reference: BlobRefRecord) {
        store.deleteBlob(reference)
    }

    suspend fun listBlobRefs(): List<BlobRefRecord> = store.listBlobRefs()

    suspend fun blobCount(): ULong = store.blobCount()

    override fun close() {
        store.close()
    }
}
