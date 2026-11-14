package com.abdownloadmanager.shared.util.category

import ir.amirab.downloader.db.FileReadResult
import ir.amirab.downloader.db.TransactionalFileSaver
import ir.amirab.util.logger.appLogger
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.io.File

class CategoryFileStorage(
    val file: File,
    val fileSaver: TransactionalFileSaver,
) : CategoryStorage {
    val lock = Mutex()
    override suspend fun setCategories(categories: List<Category>) {
        lock.withLock {
            fileSaver.writeObject(file, categories)
        }
    }

    override suspend fun getCategories(): List<Category> {
        return fileSaver.readObject(file) ?: emptyList()
    }

    /**
     * @return true only if there is a stored list of categories that we can actually use.
     *
     * If the file exists but is damaged (or unreadable) we must not report it as "set":
     * the manager would load an empty list and its auto save would then overwrite the damaged file
     * (and the user's categories with it).
     * Instead, we keep the damaged file aside and report "not set" so the manager falls back to the default categories.
     */
    override suspend fun isCategoriesSet(): Boolean {
        return lock.withLock {
            when (val result = fileSaver.read<List<Category>>(file)) {
                is FileReadResult.Ok -> true
                FileReadResult.Missing -> false
                is FileReadResult.Failure -> {
                    appLogger.e(result.cause) {
                        "Categories file ${file.path} is not usable. Falling back to default categories"
                    }
                    fileSaver.quarantine(file)
                    false
                }
            }
        }
    }
}
