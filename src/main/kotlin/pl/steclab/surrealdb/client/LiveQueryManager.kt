@file:OptIn(InternalSerializationApi::class)

package pl.steclab.surrealdb.client

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.onFailure
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.launch
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.serializer
import pl.steclab.surrealdb.model.Diff
import pl.steclab.surrealdb.result.QueryException
import pl.steclab.surrealdb.result.Result
import pl.steclab.surrealdb.result.getOrThrow
import kotlin.reflect.KClass

class LiveQueryManager(private val client: SurrealDBClient) : LiveQueryManagerInterface {
    private fun log(message: String) = client.log(message)

    override suspend fun <T : Any> live(
        table: String,
        diff: Boolean,
        type: KClass<T>,
        callback: (Diff<T>) -> Unit
    ): Result<String> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(table))
            add(JsonPrimitive(diff))
        }
        val response = client.sendRpcRequest("live", paramsJson)
        val queryUuid = response.jsonPrimitive.content
        client.registerLiveQueryCallback(queryUuid) { pair ->
            val (resultObj, jsonData) = pair
            if (diff) {
                if (jsonData is JsonArray) {
                    jsonData.forEach { patchElement ->
                        val patch = patchElement.jsonObject
                        val operation = patch["op"]?.jsonPrimitive?.content ?: "update"
                        val path = patch["path"]?.jsonPrimitive?.content
                        val rawValue = patch["value"]
                        val value = rawValue?.let {
                            try {
                                // Handle primitive values by wrapping them in an object based on path
                                val jsonElement = if (it is JsonPrimitive && path != null && path != "/") {
                                    val fieldName = path.removePrefix("/").split("/").last()
                                    buildJsonObject { put(fieldName, it) }
                                } else {
                                    it
                                }
                                client.json.decodeFromJsonElement(type.serializer(), jsonElement)
                            } catch (e: Exception) {
                                log("Failed to decode patch value: $e, input: $rawValue")
                                null
                            }
                        }
                        callback(Diff(operation, path, value))
                    }
                } else {
                    // DELETE case: result is the last state
                    val operation = "remove"
                    val path = ""
                    val value = jsonData.takeIf { it !is JsonNull }?.let {
                        try {
                            client.json.decodeFromJsonElement(type.serializer(), it)
                        } catch (e: Exception) {
                            log("Failed to decode DELETE value: $e")
                            null
                        }
                    }
                    callback(Diff(operation, path, value))
                }
            } else {
                val action = resultObj["action"]?.jsonPrimitive?.content?.lowercase() ?: "update"
                val userData = jsonData.takeIf { it !is JsonNull } ?: return@registerLiveQueryCallback
                val value = client.json.decodeFromJsonElement(type.serializer(), userData)
                callback(Diff(action, null, value))
            }
        }
        Result.Success(queryUuid)
    } catch (e: Exception) {
        Result.Error(QueryException("Failed to start live query: ${e.message}", e))
    }

    suspend inline fun <reified T : Any> live(
        table: String,
        diff: Boolean,
        noinline callback: (Diff<T>) -> Unit
    ): Result<String> = live(table, diff, T::class, callback)

    override suspend fun <T : Any> live(
        table: String,
        diff: Boolean,
        type: KClass<T>
    ): Result<Flow<Diff<T>>> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(table))
            add(JsonPrimitive(diff))
        }
        val response = client.sendRpcRequest("live", paramsJson)
        val queryUuid = response.jsonPrimitive.content
        Result.Success(callbackFlow {
            client.registerLiveQueryCallback(queryUuid) { pair ->
                val (resultObj, jsonData) = pair
                if (diff) {
                    if (jsonData is JsonArray) {
                        jsonData.forEach { patchElement ->
                            val patch = patchElement.jsonObject
                            val operation = patch["op"]?.jsonPrimitive?.content ?: "update"
                            val path = patch["path"]?.jsonPrimitive?.content
                            val rawValue = patch["value"]
                            val value = rawValue?.let {
                                try {
                                    val jsonElement = if (it is JsonPrimitive && path != null && path != "/") {
                                        val fieldName = path.removePrefix("/").split("/").last()
                                        buildJsonObject { put(fieldName, it) }
                                    } else {
                                        it
                                    }
                                    client.json.decodeFromJsonElement(type.serializer(), jsonElement)
                                } catch (e: Exception) {
                                    log("Failed to decode patch value: $e, input: $rawValue")
                                    null
                                }
                            }
                            trySend(Diff(operation, path, value)).onFailure { throwable ->
                                log("Failed to send diffData: $throwable")
                            }
                        }
                    } else {
                        val operation = "remove"
                        val path = ""
                        val value = jsonData.takeIf { it !is JsonNull }?.let {
                            try {
                                client.json.decodeFromJsonElement(type.serializer(), it)
                            } catch (e: Exception) {
                                log("Failed to decode DELETE value: $e")
                                null
                            }
                        }
                        trySend(Diff(operation, path, value)).onFailure { throwable ->
                            log("Failed to send diffData: $throwable")
                        }
                    }
                } else {
                    val action = resultObj["action"]?.jsonPrimitive?.content?.lowercase() ?: "update"
                    val userData = jsonData.takeIf { it !is JsonNull } ?: return@registerLiveQueryCallback
                    val value = client.json.decodeFromJsonElement(type.serializer(), userData)
                    trySend(Diff(action, null, value)).onFailure { throwable ->
                        log("Failed to send diffData: $throwable")
                    }
                }
            }
            awaitClose {
                CoroutineScope(coroutineContext).launch {
                    kill(queryUuid).getOrThrow()
                }
            }
        })
    } catch (e: Exception) {
        Result.Error(QueryException("Failed to start live query: ${e.message}", e))
    }

    suspend inline fun <reified T : Any> live(
        table: String,
        diff: Boolean = false
    ): Result<Flow<Diff<T>>> = live(table, diff, T::class)

    override suspend fun kill(queryUuid: String): Result<Unit> = try {
        val paramsJson = buildJsonArray { add(JsonPrimitive(queryUuid)) }
        client.sendRpcRequest("kill", paramsJson)
        client.unregisterLiveQueryCallback(queryUuid)
        Result.Success(Unit)
    } catch (e: Exception) {
        Result.Error(QueryException("Failed to kill live query: ${e.message}", e))
    }
}