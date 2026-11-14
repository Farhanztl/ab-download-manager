package ir.amirab.downloader.db

import ir.amirab.util.logger.appLogger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import java.io.File

private const val queueExtension = "json"

class DownloadQueueFileStorageDatabase(
    val fileSaver: TransactionalFileSaver,
    val queueFolder: File,
) : IDownloadQueueDatabase {
    private val lock = Mutex()
    private fun getFileOfQueue(queue: QueueModel): File {
        return getFileOfQueue(queue.id)
    }

    private fun getFileOfQueue(id: Long): File {
        return queueFolder.resolve("$id.json")
    }

    private fun getQueueFiles(): List<File> {
        return queueFolder.listFiles()
            .filter {
                it.isFile && it.extension == queueExtension
            }
    }


    override suspend fun getAllQueueIds(): List<Long> {
        return withContext(Dispatchers.IO) {
            getQueueFiles().map {
                it.name.substring(0,it.name.length-".$queueExtension".length)
            }.mapNotNull {
                it.toLongOrNull()
            }
        }
    }

    /**
     * @return null if the queue doesn't exist or its file is unusable.
     * An unusable file is set aside (not deleted): the queue manager recreates a missing main queue
     * and that would silently overwrite the only copy of the user's queue.
     */
    private fun readQueue(queueId: Long): QueueModel? {
        val file = getFileOfQueue(queueId)
        return when (val result = fileSaver.read<QueueModel>(file)) {
            is FileReadResult.Ok -> result.value
            FileReadResult.Missing -> null
            is FileReadResult.Failure -> {
                appLogger.e(result.cause) { "Queue file ${file.path} is not usable. Skipping this queue" }
                fileSaver.quarantine(file)
                null
            }
        }
    }

    override suspend fun getQueue(queueId: Long): QueueModel {
        return withContext(Dispatchers.IO) {
            requireNotNull(readQueue(queueId)) {
                "Queue with $queueId returned null"
            }
        }
    }

    override suspend fun getAllQueues(): List<QueueModel> {
        return withContext(Dispatchers.IO) {
            getAllQueueIds().mapNotNull {
                readQueue(it)
            }
        }
    }

    override suspend fun deleteAllQueues() {
        getQueueFiles().forEach {
            it.delete()
        }
    }

    override suspend fun setAllQueues(queues: List<QueueModel>) {
        lock.withLock {
            deleteAllQueues()
            queues.forEach { addQueue(it) }
        }
    }

    override suspend fun deleteQueue(queueId: Long) {
        lock.withLock {
            getFileOfQueue(queueId).delete()
        }
    }

    override suspend fun updateQueue(queue: QueueModel) {
        lock.withLock {
            withContext(Dispatchers.IO) {
                fileSaver.writeObject(
                    getFileOfQueue(queue),
                    queue
                )
            }
        }
    }

    override suspend fun addQueue(queue: QueueModel) {
        val fileOfQueue = getFileOfQueue(queue)
        withContext(Dispatchers.IO) {
            fileSaver.writeObject(
                fileOfQueue,
                queue,
            )
        }
    }

}
