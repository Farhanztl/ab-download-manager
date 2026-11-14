package ir.amirab.downloader.db

import ir.amirab.downloader.part.Parts
import ir.amirab.util.logger.appLogger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.File
import java.util.concurrent.ConcurrentHashMap

class PartListFileStorage(
    val folder: File,
    val fileSaver: TransactionalFileSaver,
) : IDownloadPartListDb {
    fun getFileForId(id: Long): File {
        val resolve = folder.resolve("$id.json")
        return resolve
    }

    override suspend fun getParts(id: Long): Parts? {
        return withContext(Dispatchers.IO) {
            fileSaver.readObject(getFileForId(id))
        }
    }

    // ids that we failed to persist their parts the last time, so we don't repeat the same error every second
    private val failingIds: MutableSet<Long> = ConcurrentHashMap.newKeySet()

    /**
     * This intentionally doesn't throw if writing fails: it's called from critical paths
     * (pause / finish / cancel) that must still be able to report their state.
     * But it must not fail silently either: the download keeps going but may not be resumable.
     */
    override suspend fun setParts(id: Long, parts: Parts) {
        withContext(Dispatchers.IO) {
            try {
                val file = getFileForId(id)
                fileSaver.writeObject(file, parts)
                if (failingIds.remove(id)) {
                    appLogger.i { "Saving parts of download $id works again" }
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                if (failingIds.add(id)) {
                    appLogger.e(e) { "Can't save parts of download $id. It may not be resumable" }
                }
            }
        }
    }

    override suspend fun removeParts(id: Long) {
        getFileForId(id).delete()
    }

    override suspend fun clear() {
        kotlin.runCatching {
            folder.listFiles()
        }.getOrNull()?.let {
            for (file in it) {
                file.delete()
            }
        }
    }
}
