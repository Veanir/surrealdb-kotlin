package pl.steclab.surrealdb.client

import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.websocket.*
import io.ktor.websocket.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.serialization.json.*
import pl.steclab.surrealdb.result.DatabaseException
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds

class SurrealDBClient(private val config: SurrealDBClientConfig) {
    private val client = HttpClient(CIO) { install(WebSockets) }
    private val requestIdCounter = AtomicInteger(0)
    private val pendingRequests = mutableMapOf<Int, CompletableDeferred<JsonElement>>()
    private val liveQueryCallbacks = mutableMapOf<String, (Pair<JsonObject, JsonElement>) -> Unit>()
    private val sendChannel = Channel<String>(capacity = 100)
    private var webSocketSession: DefaultClientWebSocketSession? = null
    private var connectionJob: Job? = null
    internal val json = Json { ignoreUnknownKeys = true }

    internal fun log(message: String) { if (config.verboseLogging) println(message) } // Changed to internal

    suspend fun connect() {
        connectionJob = CoroutineScope(Dispatchers.IO + CoroutineExceptionHandler { _, e ->
            log("Connection coroutine exception: $e")
        }).launch {
            try {
                client.webSocket(config.url) {
                    webSocketSession = this
                    log("WebSocket connection established to ${config.url}")
                    launch {
                        for (message in sendChannel) {
                            send(message)
                        }
                    }
                    incoming.consumeEach { frame ->
                        if (frame is Frame.Text) handleMessage(frame.readText())
                    }
                }
            } catch (e: Exception) {
                if (e !is CancellationException) {
                    throw DatabaseException("Failed to connect to ${config.url}", e)
                }
            }
        }
        try {
            withTimeout(config.connectionTimeout?.inWholeMilliseconds ?: 5.seconds.inWholeMilliseconds) {
                while (webSocketSession == null && connectionJob?.isActive == true) delay(10)
                if (webSocketSession == null) throw DatabaseException("Connection timed out")
            }
        } catch (e: TimeoutCancellationException) {
            disconnect()
            throw DatabaseException("Failed to connect to ${config.url}: Timeout", e)
        }
    }

    suspend fun disconnect() {
        try {
            connectionJob?.let {
                if (it.isActive) {
                    it.cancel("Disconnecting client")
                    it.join()
                }
            }
            webSocketSession?.close(CloseReason(CloseReason.Codes.NORMAL, "Client disconnect"))
            sendChannel.close()
            synchronized(pendingRequests) { pendingRequests.clear() }
            synchronized(liveQueryCallbacks) { liveQueryCallbacks.clear() }
            client.close()
            log("Disconnected from ${config.url}")
        } catch (e: Exception) {
            log("Error during disconnect: ${e.message}")
        } finally {
            webSocketSession = null
            connectionJob = null
        }
    }

    private fun handleMessage(message: String) {
        try {
            log("Received WebSocket message: $message")
            val jsonElement = json.parseToJsonElement(message)
            when {
                jsonElement is JsonObject && jsonElement.containsKey("id") -> {
                    val id = jsonElement["id"]?.jsonPrimitive?.intOrNull ?: return
                    val result = jsonElement["result"] ?: JsonNull
                    val error = jsonElement["error"]
                    val deferred = synchronized(pendingRequests) { pendingRequests.remove(id) } ?: return
                    if (error != null && error != JsonNull) {
                        val errorMessage = error.jsonObject["message"]?.jsonPrimitive?.content ?: error.toString()
                        deferred.completeExceptionally(DatabaseException("RPC error: $errorMessage"))
                    } else {
                        deferred.complete(result)
                    }
                }
                jsonElement is JsonObject && jsonElement.containsKey("result") -> {
                    val resultObj = jsonElement["result"]?.jsonObject ?: return
                    val queryUuid = resultObj["id"]?.jsonPrimitive?.content ?: return
                    val data = resultObj["result"] ?: JsonNull
                    log("Live query message received: $jsonElement")
                    synchronized(liveQueryCallbacks) { liveQueryCallbacks[queryUuid] }?.invoke(Pair(resultObj, data))
                }
                else -> log("Unhandled message type: $jsonElement")
            }
        } catch (e: Exception) {
            log("Error parsing message: $message, exception: $e")
        }
    }

    internal suspend fun sendRpcRequest(method: String, params: JsonArray): JsonElement {
        val requestId = requestIdCounter.incrementAndGet()
        val request = buildJsonObject {
            put("id", requestId)
            put("method", method)
            put("params", params)
        }
        val deferred = CompletableDeferred<JsonElement>()
        synchronized(pendingRequests) { pendingRequests[requestId] = deferred }
        try {
            log("Sending request $request")
            sendChannel.send(request.toString())
        } catch (e: Exception) {
            synchronized(pendingRequests) { pendingRequests.remove(requestId) }
            throw DatabaseException("Failed to send RPC request '$method': ${e.message}", e)
        }
        return deferred.await()
    }

    internal fun registerLiveQueryCallback(queryUuid: String, callback: (Pair<JsonObject, JsonElement>) -> Unit) {
        synchronized(liveQueryCallbacks) { liveQueryCallbacks[queryUuid] = callback }
    }

    internal fun unregisterLiveQueryCallback(queryUuid: String) {
        synchronized(liveQueryCallbacks) { liveQueryCallbacks.remove(queryUuid) }
    }
}