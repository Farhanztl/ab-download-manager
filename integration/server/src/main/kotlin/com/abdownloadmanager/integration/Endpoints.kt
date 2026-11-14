package com.abdownloadmanager.integration

import com.abdownloadmanager.integration.model.AddDownloadsFromIntegration
import com.abdownloadmanager.integration.model.ApiQueueModel
import com.abdownloadmanager.integration.model.NewDownloadTask
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.auth.Authentication
import io.ktor.server.auth.apikey.apiKey
import io.ktor.server.auth.authenticate
import io.ktor.server.request.receiveText
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.json.Json
import java.security.MessageDigest

private data class AppPrincipal(val key: String)

internal fun Application.setupRouting(
    json: Json,
    integrationHandler: IntegrationHandler,
    settings: IntegrationSettings,
) {
    val apiKey = settings.apiKey
    // must be first: it protects every route, including the ones that don't need an api key
    installRequestGuard()
    install(Authentication) {
        apiKey {
            validate { receivedKey ->
                // constant time comparison
                if (apiKey != null && MessageDigest.isEqual(receivedKey.toByteArray(), apiKey.toByteArray())) {
                    AppPrincipal(receivedKey)
                } else null
            }
            skipWhen {
                apiKey == null
            }
        }
    }
    routing {
        authenticate {
            post("/add") {
                val itemsToAdd = kotlin.runCatching {
                    val message = call.receiveText()
                    AddDownloadsFromIntegration.createFromRequest(
                        json = json,
                        jsonData = message
                    )
                }
                itemsToAdd.onFailure { it.printStackTrace() }
                itemsToAdd.getOrThrow().let { newImportRequest ->
                    integrationHandler.addDownloadByGui(
                        AddDownloadsFromIntegration(
                            newImportRequest.items,
                            newImportRequest.options,
                        )
                    )
                }
                call.respondText("OK")
            }
            get("/queues") {
                val queues = integrationHandler.listQueues()
                val jsonResponse = json.encodeToString(ListSerializer(ApiQueueModel.serializer()), queues)
                call.respondText(jsonResponse)
            }
            post("/start-headless-download") {
                val itemsToAdd = kotlin.runCatching {
                    val message = call.receiveText()
                    json.decodeFromString<NewDownloadTask>(message)
                }
                itemsToAdd.onFailure { it.printStackTrace() }
                val task = itemsToAdd.getOrThrow()
                try {
                    // without an api key anyone who can reach this port is "the caller"
                    integrationHandler.addDownload(task, isTrustedCaller = apiKey != null)
                } catch (e: IntegrationRequestRejectedException) {
                    call.respondText(e.message.orEmpty(), status = HttpStatusCode.Forbidden)
                    return@post
                }
                call.respondText("OK")
            }
            post("/ping") {
                call.respondText("pong")
            }
        }
    }
}
