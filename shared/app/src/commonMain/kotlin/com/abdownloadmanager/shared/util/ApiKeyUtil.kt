package com.abdownloadmanager.shared.util

import java.security.SecureRandom

object ApiKeyUtil {
    fun isValidKey(key: String): Boolean {
        return key.isNotBlank()
    }

    const val DEFAULT_KEY_LENGTH = 24
    private const val CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

    // this key protects the local api so it must come from a cryptographically secure source
    private val secureRandom by lazy { SecureRandom() }

    fun generateKey(length: Int = DEFAULT_KEY_LENGTH): String {
        return buildString(length) {
            repeat(length) {
                append(CHARS[secureRandom.nextInt(CHARS.length)])
            }
        }
    }
}
