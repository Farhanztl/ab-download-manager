package ir.amirab.util

import ir.amirab.util.osfileutil.FileUtils
import java.io.File

object PathValidator {
    fun canWriteToThisPath(path: String): Boolean {
        return FileUtils.canWriteInThisFolder(path)
    }

    /**
     * @return true if [path] is [parent] or is inside it.
     * `..` and symbolic links are resolved first, so they can't be used to escape [parent].
     */
    fun isInside(path: String, parent: String): Boolean {
        return runCatching {
            File(path).canonicalFile.toPath()
                .startsWith(File(parent).canonicalFile.toPath())
        }.getOrDefault(false)
    }

    fun isInsideAny(path: String, parents: Collection<String>): Boolean {
        return parents.any { isInside(path, it) }
    }

    fun isValidPath(path: String): Boolean {
        if (path.isEmpty()) return false
        return runCatching {
            File(path).canonicalFile
            true
        }.getOrElse { false }
    }
}
