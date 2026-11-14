package com.abdownloadmanager.integration

import com.abdownloadmanager.integration.model.AddDownloadsFromIntegration
import com.abdownloadmanager.integration.model.ApiQueueModel
import com.abdownloadmanager.integration.model.NewDownloadTask

interface IntegrationHandler{
    /**
     * meant to be used by the browser / or when the app needs to use gui
     * the app gui should be involved
     */
    suspend fun addDownloadByGui(request: AddDownloadsFromIntegration)


    fun listQueues(): List<ApiQueueModel>

    /**
     * manually add download
     * all the necessary inputs provided by the cli/api
     * simply add the download using download system
     *
     * @param isTrustedCaller false if the request came from an unauthenticated source (e.g. the local http api when
     * the api key is not set). Anyone who can reach such a source (any local process / web page) can send it,
     * so it must not be able to make the app write wherever it wants.
     * @throws IntegrationRequestRejectedException if the request is not allowed for this caller
     */
    suspend fun addDownload(task: NewDownloadTask, isTrustedCaller: Boolean): Long
}

/**
 * The request is valid but not allowed for the caller.
 */
class IntegrationRequestRejectedException(message: String) : Exception(message)
