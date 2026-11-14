package ir.amirab.downloader

import arrow.core.Some
import ir.amirab.downloader.db.IDownloadListDb
import ir.amirab.downloader.db.IDownloadPartListDb
import ir.amirab.downloader.downloaditem.*
import ir.amirab.downloader.downloaditem.contexts.DuplicateRemoval
import ir.amirab.downloader.downloaditem.contexts.RemovedBy
import ir.amirab.downloader.downloaditem.DownloadJobStatus
import ir.amirab.downloader.downloaditem.DownloadStatus
import ir.amirab.downloader.utils.DuplicateFilterByPath
import ir.amirab.downloader.utils.EmptyFileCreator
import ir.amirab.downloader.utils.FileNameUtil
import ir.amirab.downloader.utils.OnDuplicateStrategy.*
import ir.amirab.util.FileNameValidator
import ir.amirab.util.PathValidator
import ir.amirab.util.logger.appLogger
import ir.amirab.util.suspendGuardedEntry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import ir.amirab.downloader.utils.speedlimiter.SpeedLimiter
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

class DownloadManager(
    val dlListDb: IDownloadListDb,
    val partListDb: IDownloadPartListDb,
    val settings: DownloadSettings,
    val emptyFileCreator: EmptyFileCreator,
    private val downloaderRegistry: DownloaderRegistry,
    val downloadDataFolder: File
) : DownloadManagerMinimalControl {

    val scope = CoroutineScope(SupervisorJob())

    private val bootGuard = suspendGuardedEntry()
    suspend fun awaitBoot() {
        bootGuard.awaitDone()
    }

    //make ready to resume download
    suspend fun boot() {
        bootGuard.action {
            createJobForPendingDownloads()
        }
    }

    private val contextContainer = ContextProvider()

    private suspend fun createJobForPendingDownloads() {
        dlListDb.getAll().filter {
            it.status != DownloadStatus.Completed
        }.forEach {
            createJob(it).boot()
        }
    }

    // copy on write list: readers (the monitor, queues, ...) use whatever snapshot they see without locking,
    // writers must hold [jobModificationLock]
    @Volatile
    var downloadJobs = listOf<DownloadJob>()
        private set

    private val dbAddSync = Mutex()

    suspend fun addDownload(
        props: NewDownloadItemProps
    ): Long {
        val newItem = props.downloadItem
        val onDuplicateStrategy = props.onDuplicateStrategy
        val context = props.context
        val extraConfig = props.extraConfig
        newItem.validateItem()
        require(PathValidator.isValidPath(newItem.folder)) { "folder of new download is not valid: ${newItem.folder}" }
        require(PathValidator.canWriteToThisPath(newItem.folder)) { "can't write to this new download's folder: ${newItem.folder}" }
        require(FileNameValidator.isValidFileName(newItem.name)) { "name of new download is not valid: ${newItem.name}" }
//        thisLogger().info("adding download")
        val job = dbAddSync.withLock {
            val allDownloads = dlListDb.getAll()
            val duplicateFinder = DuplicateFilterByPath(File(newItem.folder, newItem.name))
            val foundItems = allDownloads.filter(duplicateFinder::isDuplicate)
            var removedItems = emptyList<IDownloadItem>()
            if (foundItems.isNotEmpty()) {
                when (onDuplicateStrategy) {
                    AddNumbered -> {
                        //we do nothing here instead we increment file name after all if necessary
                    }

                    OverrideDownload -> {
                        foundItems.forEach {
                            deleteDownload(it.id, { true }, RemovedBy(DuplicateRemoval))
                        }
                        removedItems = foundItems
                    }

                    Abort -> {
                        error("Aborting add download that already exists")
                    }
                }
            }

            val name = FileNameUtil.numberedIfExists(
                File(newItem.folder, newItem.name)
            ).first { candidateNewFile ->
                val withSameDestination = allDownloads
                    .filter { it !in removedItems }
                    .find {
                        it.name == candidateNewFile.name && it.folder == candidateNewFile.parent
                    }
                withSameDestination == null
            }.name

            val id = dlListDb.getLastId() + 1
            val dateAdded = newItem.dateAdded
                .takeIf { it != 0L }
                ?: System.currentTimeMillis()

            val downloadItem = newItem.copy(
                id = Some(id),
                name = Some(name),
                dateAdded = Some(dateAdded),
                startTime = Some(null),
                completeTime = Some(null),
                status = Some(DownloadStatus.Added)
            )
            dlListDb.add(downloadItem)
            createJob(downloadItem)
                .apply { boot() }
                .apply {
                    extraConfig?.let {
                        extraConfigsReceived(it)
                    }
                }
        }
        contextContainer.setContext(job.id, context)
        onDownloadAdded(job.downloadItem)
//        thisLogger().info("this download added $downloadItem")
//        println("download created ${job.id}")
        return job.id
    }

    private val jobModificationLock = Any()

    /**
     * Creates a job for this item and registers it.
     * If a job for this item is already registered it is returned instead,
     * (so two concurrent callers can't end up with two jobs downloading the same item).
     */
    private fun createJob(downloadItem: IDownloadItem): DownloadJob {
        synchronized(jobModificationLock) {
            downloadJobs.find { it.id == downloadItem.id }?.let {
                return it
            }
            val job = downloaderRegistry.createJob(
                downloadItem,
                this,
            )
//        thisLogger().info("download job for $id created")
            downloadJobs = downloadJobs + job
            return job
        }
    }

    suspend fun deleteDownload(
        id: Long,
        alsoRemoveFile: (IDownloadItem) -> Boolean,
        context: DownloadItemContext = EmptyContext,
    ) {
        try {
            pause(id)
        } catch (e: Exception) {
            // we are going to delete it anyway, but if *we* were cancelled we must not continue
            currentCoroutineContext().ensureActive()
        }
        val itemToDelete = dlListDb.getById(id) ?: return
        val job = getDownloadJob(id) ?: run {
            createJob(itemToDelete).apply { boot() }
        }
        // at this point: job will be created (and booted) if it was not created before
        contextContainer.updateContext(id) { it + context }
        job.downloadRemoved(
            removeOutputFile = if (itemToDelete.status == DownloadStatus.Completed) {
                alsoRemoveFile(itemToDelete)
            } else {
                // always remove file if download is not finished!
                true
            },
        )
        deleteJob(job.id)
        dlListDb.remove(itemToDelete)
        partListDb.removeParts(id)
        emitEvent(
            DownloadManagerEvents.OnJobRemoved(itemToDelete, contextContainer.getContext(id))
        )
        contextContainer.removeContext(id)
    }

    private fun deleteJob(
        id: Long,
    ) {
        synchronized(jobModificationLock) {
            val jobToDelete = downloadJobs.find {
                it.id == id
            }
            jobToDelete?.let {
                it.close()
                downloadJobs = downloadJobs.minusElement(it)
            }
        }
    }

    suspend fun pause(id: Long, context: DownloadItemContext = EmptyContext) {
        val job = getDownloadJob(id) ?: return
        contextContainer.updateContext(id) { it + context }
        job.pause()
    }

    suspend fun resume(id: Long, context: DownloadItemContext = EmptyContext) {
        val job = getDownloadJob(id) ?: run {
            dlListDb.getById(id)?.let {
                createJob(it)
            }
        }
        job?.let {
            contextContainer.updateContext(id) { it + context }
            it.resume()
        }
    }

    suspend fun reset(id: Long, context: DownloadItemContext = EmptyContext) {
        val job = getDownloadJob(id) ?: run {
            dlListDb.getById(id)?.let {
                createJob(it)
            }
        }
        job?.let {
            contextContainer.updateContext(id) { it + context }
            it.reset()
        }
    }

    private fun getDownloadJob(id: Long): DownloadJob? {
//        thisLogger().info("finding job for $id")
        return downloadJobs.find {
            it.id == id
        }.also {
            if (it == null) {
//                thisLogger().info("there is no job for dl_$id")
            } else {
//                thisLogger().info("job found for dl_$id")
            }
        }
    }

    suspend fun getDownloadList(): List<IDownloadItem> {
        return dlListDb.getAll()
    }

    fun onDownloadResuming(downloadItem: IDownloadItem) {
        emitEvent(
            DownloadManagerEvents.OnJobStarting(
                downloadItem,
                contextContainer.getContext(downloadItem.id)
            )
        )
    }

    fun onDownloadResumed(downloadItem: IDownloadItem) {
        emitEvent(
            DownloadManagerEvents.OnJobStarted(
                downloadItem,
                contextContainer.getContext(downloadItem.id)
            )
        )
    }

    fun onDownloadAdded(downloadItem: IDownloadItem) {
        emitEvent(
            DownloadManagerEvents.OnJobAdded(
                downloadItem,
                contextContainer.getContext(downloadItem.id)
            )
        )
    }

    fun onDownloadCanceled(downloadItem: IDownloadItem, throwable: Throwable) {
        emitEvent(
            DownloadManagerEvents.OnJobCanceled(
                downloadItem,
                contextContainer.getContext(downloadItem.id), throwable
            )
        )
    }

    fun onDownloadFinished(downloadItem: IDownloadItem) {
        scope.launch {
            emitEvent(
                DownloadManagerEvents.OnJobCompleted(
                    downloadItem,
                    contextContainer.getContext(downloadItem.id)
                )
            )
            deleteJob(downloadItem.id)
        }
    }

    fun onDownloadItemChange(downloadItem: IDownloadItem) {
        scope.launch {
            emitEvent(
                DownloadManagerEvents.OnJobChanged(
                    downloadItem,
                    contextContainer.getContext(downloadItem.id)
                )
            )
        }
    }

    override suspend fun startJob(id: Long, context: DownloadItemContext) {
        resume(id, context)
    }

    override suspend fun stopJob(id: Long, context: DownloadItemContext) {
        pause(id, context)
    }

    override fun canActivateJob(id: Long): Boolean {
        val job = downloadJobs.find { id == it.id }
        val status = job?.status?.value
//        println("job status $status")
        return status is DownloadJobStatus.CanBeResumed
    }

    suspend fun stopAll(
        context: DownloadItemContext = EmptyContext,
    ) {
        downloadJobs.filter {
            it.status.value == DownloadJobStatus.Downloading
        }.map {
            scope.async {
                pause(it.id, context)
            }
        }.awaitAll()
    }

    fun getActiveCount(): Int {
        return downloadJobs.filter {
            it.status.value is DownloadJobStatus.IsActive
        }.size
    }

    fun calculateOutputFile(downloadItem: IDownloadItem): File {
        return File(downloadItem.folder, downloadItem.name)
    }

    fun getJobStatusOf(id: Long): DownloadJobStatus? {
        return downloadJobs.find {
            it.id == id
        }?.status?.value
    }

    // Listeners (queues, monitor, UI ...) rely on these events to stay in sync (e.g. a queue frees its slot on completion).
    // The buffer must be big enough to absorb bursts (e.g. stopping a lot of downloads at once) while a listener is busy.
    override val listOfJobsEvents: MutableSharedFlow<DownloadManagerEvents> =
        MutableSharedFlow(extraBufferCapacity = EVENT_BUFFER_CAPACITY)

    private val _droppedEventCount = AtomicLong(0)

    /** how many events we couldn't deliver because the buffer was full. It should always be 0. */
    val droppedEventCount: Long get() = _droppedEventCount.get()

    private fun emitEvent(event: DownloadManagerEvents) {
        // tryEmit only fails when the buffer is full. In that case, the event is lost for every listener,
        // that must be visible.
        if (!listOfJobsEvents.tryEmit(event)) {
            _droppedEventCount.incrementAndGet()
            appLogger.w {
                "Download event buffer is full! dropped ${event::class.simpleName} of download ${event.downloadItem.id}"
            }
        }
    }

    //global speed limiter
    internal val speedLimiter = SpeedLimiter()
    fun limitGlobalSpeed(bytePerSecond: Long) {
        speedLimiter.bytesPerSecond(bytePerSecond)
    }

    fun reloadSetting() {
        for (downloadJob in downloadJobs) {
            downloadJob.reloadSettings()
        }
    }

    suspend fun updateDownloadItem(
        id: Long,
        downloadJobExtraConfig: DownloadJobExtraConfig?,
        updater: (IDownloadItem) -> Unit,
    ) {
        var wasCreated = false
        val job = getDownloadJob(id) ?: run {
            dlListDb.getById(id)?.let {
                wasCreated = true
                createJob(it)
            }
        } ?: return
        val updated = job.changeConfig(updater, downloadJobExtraConfig)
        if (wasCreated && updated.status == DownloadStatus.Completed) {
            deleteJob(job.id)
        }
        onDownloadItemChange(updated)
    }

}

/**
 * contexts are read and written from many threads (jobs, queues, UI actions) so it must be thread safe.
 * an item without a (meaningful) context has no entry.
 */
private class ContextProvider {
    private val contexts = ConcurrentHashMap<Long, DownloadItemContext>()
    fun getContext(id: Long): DownloadItemContext {
        return contexts[id] ?: EmptyContext
    }

    fun setContext(id: Long, context: DownloadItemContext) {
        if (context == EmptyContext) {
            removeContext(id)
            return
        }
        contexts[id] = context
    }

    fun removeContext(id: Long) {
        contexts.remove(id)
    }

    fun updateContext(id: Long, block: (DownloadItemContext) -> DownloadItemContext) {
        // atomic read-modify-write. returning null removes the entry
        contexts.compute(id) { _, current ->
            block(current ?: EmptyContext).takeIf { it != EmptyContext }
        }
    }
}

private const val EVENT_BUFFER_CAPACITY = 1024
