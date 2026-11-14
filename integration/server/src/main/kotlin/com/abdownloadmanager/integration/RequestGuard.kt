package com.abdownloadmanager.integration

import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCallPipeline
import io.ktor.server.application.call
import io.ktor.server.response.respondText

/**
 * The server only listens on the loopback interface, but that doesn't mean that only trusted software talks to it:
 * any web page that the user opens can make the browser send requests to `127.0.0.1`.
 * - A cross-origin request (a simple `POST` with a `text/plain` body needs no preflight) always carries an `Origin`
 * header. Web pages can't remove or forge it, so we reject every origin except browser extensions
 * (our own extension is one of them).
 * - A page can also make its own domain point to `127.0.0.1` (DNS rebinding), which makes requests same-origin
 * for the browser. Those requests carry our address in the URL but the attacker's domain in the `Host` header.
 *
 * Requests without an `Origin` (curl, scripts, native apps) are not affected.
 */
internal object RequestGuard {
    private val allowedHostNames = setOf("127.0.0.1", "localhost", "::1")
    private val allowedOriginSchemes = setOf(
        "chrome-extension",
        "moz-extension",
        "safari-web-extension",
    )

    fun isAllowedHost(hostHeader: String?): Boolean {
        // HTTP/1.0 clients may omit it. A browser (DNS rebinding) always sends it.
        hostHeader ?: return true
        val host = extractHostName(hostHeader) ?: return false
        return host.lowercase() in allowedHostNames
    }

    fun isAllowedOrigin(originHeader: String?): Boolean {
        originHeader ?: return true
        // `Origin: null` (sandboxed frames, file://, redirects ...) has no scheme and is rejected too
        val scheme = originHeader.substringBefore("://", missingDelimiterValue = "")
        return scheme.lowercase() in allowedOriginSchemes
    }

    /**
     * "127.0.0.1:15151" -> "127.0.0.1"
     * "[::1]:15151" -> "::1"
     */
    private fun extractHostName(hostHeader: String): String? {
        val value = hostHeader.trim()
        if (value.startsWith("[")) {
            val end = value.indexOf(']')
            if (end == -1) return null
            return value.substring(1, end)
        }
        return value.substringBefore(':')
    }
}

internal fun Application.installRequestGuard() {
    intercept(ApplicationCallPipeline.Plugins) {
        val headers = call.request.headers
        val allowed = RequestGuard.isAllowedHost(headers[HttpHeaders.Host]) &&
                RequestGuard.isAllowedOrigin(headers[HttpHeaders.Origin])
        if (!allowed) {
            call.respondText("Forbidden", status = HttpStatusCode.Forbidden)
            finish()
        }
    }
}
