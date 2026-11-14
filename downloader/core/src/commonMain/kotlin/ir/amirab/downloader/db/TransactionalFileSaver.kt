package ir.amirab.downloader.db

import ir.amirab.util.logger.appLogger
import ir.amirab.util.tryAtomicMove
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer
import okio.FileSystem
import okio.Path.Companion.toOkioPath
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.nio.file.Files
import java.util.concurrent.ConcurrentHashMap

/**
 * Result of reading a persisted object.
 * It distinguishes "there is nothing stored" from "something is stored but we couldn't use it",
 * so callers can avoid treating a damaged file like an empty one (and overwriting it with defaults).
 */
sealed interface FileReadResult<out T> {
    data class Ok<T>(val value: T) : FileReadResult<T>

    /** The file does not exist. */
    data object Missing : FileReadResult<Nothing>

    /** The file exists but we couldn't use it. */
    sealed interface Failure : FileReadResult<Nothing> {
        val cause: Throwable
    }

    /** The file was read but its content can't be decoded (truncated, damaged, unknown format ...). */
    data class Corrupt(override val cause: Throwable) : Failure

    /** The file exists but could not be read (permissions, locked by another process ...). */
    data class IoError(override val cause: Throwable) : Failure
}

class TransactionalFileSaver(
    val json: Json,
) {
    fun getBakFile(file: File) = File("$file.tmp")
    inline fun <reified T> writeObject(file: File, t: T) {
        val text = json.encodeToString(t)
        writeText(file, text)
    }

    fun <T> writeObject(file: File, t: T, kSerializer: KSerializer<T>) {
        val text = json.encodeToString(kSerializer, t)
        writeText(file, text)
    }

    fun writeText(file: File, text: String) {
        val bakFile = getBakFile(file)
        kotlin.runCatching {
            FileSystem.SYSTEM.write(
                file = bakFile.toOkioPath()
            ) {
                writeUtf8(text)
            }
        }.onSuccess {
            bakFile.tryAtomicMove(file)
        }.getOrThrow()
    }

    fun readText(file: File): String? {
        return runCatching {
            FileSystem.SYSTEM.read(file.toOkioPath()) {
                readUtf8()
            }
        }.getOrNull()
    }

    /**
     * Reads and decodes [file] and tells why it failed if it did.
     * This never logs and never touches the file.
     */
    fun <T> read(file: File, serializer: KSerializer<T>): FileReadResult<T> {
        if (!file.exists()) {
            return FileReadResult.Missing
        }
        val text = try {
            FileSystem.SYSTEM.read(file.toOkioPath()) {
                readUtf8()
            }
        } catch (e: FileNotFoundException) {
            // Careful: it's not only thrown for missing files, FileInputStream also uses it for
            // "Permission denied" and "Is a directory". Only a file that is really gone is "missing",
            // an unreadable file must not look like a missing one (the caller would replace it with defaults).
            return if (file.exists()) FileReadResult.IoError(e) else FileReadResult.Missing
        } catch (e: IOException) {
            return FileReadResult.IoError(e)
        }
        return try {
            FileReadResult.Ok(json.decodeFromString(serializer, text))
        } catch (e: IllegalArgumentException) {
            // SerializationException is an IllegalArgumentException
            FileReadResult.Corrupt(e)
        }
    }

    inline fun <reified T> read(file: File): FileReadResult<T> {
        return read(file, json.serializersModule.serializer<T>())
    }

    /**
     * Like [read] but flattens the result into a nullable value.
     * A failure (other than a missing file) is logged, once per file version, instead of being silently ignored.
     * It never touches the file.
     * Use [read] when "damaged" must be handled differently from "missing".
     */
    fun <T> readObject(file: File, serializer: KSerializer<T>): T? {
        return when (val result = read(file, serializer)) {
            is FileReadResult.Ok -> result.value
            FileReadResult.Missing -> null
            is FileReadResult.Corrupt -> {
                logFailureOnce(file, result.cause, isCorrupt = true)
                null
            }

            is FileReadResult.IoError -> {
                logFailureOnce(file, result.cause, isCorrupt = false)
                null
            }
        }
    }

    inline fun <reified T> readObject(file: File): T? {
        return readObject(file, json.serializersModule.serializer<T>())
    }

    /**
     * Sets a damaged or unreadable [file] aside (`<name>.corrupt-<timestamp>`) so that the caller can
     * continue with defaults without overwriting the only copy of the user's data.
     * If the file can't be moved (e.g. it's locked) we try to copy it instead.
     * @return the new location of the saved copy or null if we couldn't keep it.
     */
    fun quarantine(file: File): File? {
        val target = File(file.absoluteFile.parentFile, "${file.name}.corrupt-${System.currentTimeMillis()}")
        try {
            Files.move(file.toPath(), target.toPath())
            appLogger.w { "Moved unusable file ${file.path} to ${target.path}" }
            return target
        } catch (e: Exception) {
            appLogger.w(e) { "Can't move unusable file ${file.path}, trying to copy it" }
        }
        return try {
            Files.copy(file.toPath(), target.toPath())
            appLogger.w { "Copied unusable file ${file.path} to ${target.path}" }
            target
        } catch (e: Exception) {
            appLogger.e(e) { "Can't keep a copy of unusable file ${file.path}; its content may be overwritten" }
            null
        }
    }

    // files that we already complained about; many of them are read repeatedly (e.g. the download list)
    private val reportedFailures: MutableSet<String> = ConcurrentHashMap.newKeySet()

    private fun logFailureOnce(file: File, cause: Throwable, isCorrupt: Boolean) {
        // include modified time so that we report again if a file gets damaged again after it was fixed
        val key = "${file.absolutePath}@${file.lastModified()}"
        if (!reportedFailures.add(key)) {
            return
        }
        if (isCorrupt) {
            appLogger.e(cause) { "Can't decode ${file.path}; ignoring its content" }
        } else {
            appLogger.w(cause) { "Can't read ${file.path}; ignoring it for now" }
        }
    }
}
