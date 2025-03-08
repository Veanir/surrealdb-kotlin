package pl.steclab.surrealdb

import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.websocket.*
import io.ktor.websocket.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.onFailure
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.*
import kotlinx.serialization.serializer
import java.util.concurrent.atomic.AtomicInteger
import kotlin.reflect.KClass

/**
 * Configuration for the SurrealDB client.
 */
data class SurrealDBClientConfig(
    var url: String = "ws://localhost:8000/rpc",
    var namespace: String? = null,
    var database: String? = null,
    var credentials: SignInParams? = null,
    var verboseLogging: Boolean = false
)

/**
 * Scope utility to manage the client lifecycle.
 */
fun <T> withSurrealDB(config: SurrealDBClientConfig, block: suspend SurrealDBClient.() -> T): T {
    val client = SurrealDBClient(config)
    return runBlocking {
        try {
            client.connect()
            config.credentials?.let { client.signin(it).getOrThrow() }
            config.namespace?.let { ns -> config.database?.let { db -> client.use(ns, db).getOrThrow() } }
            client.block()
        } finally {
            client.disconnect()
        }
    }
}

/**
 * The concrete SurrealDB client implementation.
 */
@OptIn(InternalSerializationApi::class)
class SurrealDBClient(private val config: SurrealDBClientConfig) {
    private val client = HttpClient(CIO) { install(WebSockets) }
    private val requestIdCounter = AtomicInteger(0)
    private val pendingRequests = mutableMapOf<Int, CompletableDeferred<JsonElement>>()
    private val liveQueryCallbacks = mutableMapOf<String, (JsonElement) -> Unit>()
    private val sendChannel = Channel<String>(capacity = 100)
    private var webSocketSession: DefaultClientWebSocketSession? = null
    private var connectionJob: Job? = null
    internal val json = Json { ignoreUnknownKeys = true }

    private fun log(message: String) { if (config.verboseLogging) println(message) }

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
            withTimeout(1000) {
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
                    synchronized(liveQueryCallbacks) { liveQueryCallbacks[queryUuid] }?.invoke(data)
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
            sendChannel.send(request.toString())
        } catch (e: Exception) {
            synchronized(pendingRequests) { pendingRequests.remove(requestId) }
            throw DatabaseException("Failed to send RPC request '$method': ${e.message}", e)
        }
        return deferred.await()
    }

    private fun registerLiveQueryCallback(queryUuid: String, callback: (JsonElement) -> Unit) {
        synchronized(liveQueryCallbacks) { liveQueryCallbacks[queryUuid] = callback }
    }

    private fun unregisterLiveQueryCallback(queryUuid: String) {
        synchronized(liveQueryCallbacks) { liveQueryCallbacks.remove(queryUuid) }
    }

    // Session Management
    suspend fun use(namespace: String? = null, database: String?): Result<JsonElement> = try {
        val paramsJson = buildJsonArray {
            add(namespace?.let { JsonPrimitive(it) } ?: JsonNull)
            add(database?.let { JsonPrimitive(it) } ?: JsonNull)
        }
        Result.Success(sendRpcRequest("use", paramsJson))
    } catch (e: Exception) {
        Result.Error(DatabaseException("Failed to set namespace/database: ${e.message}", e))
    }

    suspend fun info(): Result<JsonElement> = try {
        Result.Success(sendRpcRequest("info", JsonArray(emptyList())))
    } catch (e: Exception) {
        Result.Error(DatabaseException("Failed to retrieve info: ${e.message}", e))
    }

    suspend fun version(): Result<JsonElement> = try {
        Result.Success(sendRpcRequest("version", JsonArray(emptyList())))
    } catch (e: Exception) {
        Result.Error(DatabaseException("Failed to retrieve version: ${e.message}", e))
    }

    // Authentication
    suspend fun signup(params: SignUpParams): Result<String?> = try {
        val paramsJson = buildJsonArray { add(json.encodeToJsonElement(params)) }
        val result = sendRpcRequest("signup", paramsJson)
        Result.Success(if (result is JsonNull) null else result.jsonPrimitive.content)
    } catch (e: Exception) {
        Result.Error(AuthenticationException("Signup failed: ${e.message}", e))
    }

    suspend fun signin(params: SignInParams): Result<String?> = try {
        val paramsJson = buildJsonArray { add(json.encodeToJsonElement(params)) }
        val result = sendRpcRequest("signin", paramsJson)
        Result.Success(if (result is JsonNull) null else result.jsonPrimitive.content)
    } catch (e: Exception) {
        Result.Error(AuthenticationException("Signin failed: ${e.message}", e))
    }

    suspend fun authenticate(token: String): Result<Unit> = try {
        val paramsJson = buildJsonArray { add(JsonPrimitive(token)) }
        sendRpcRequest("authenticate", paramsJson)
        Result.Success(Unit)
    } catch (e: Exception) {
        Result.Error(AuthenticationException("Authentication failed: ${e.message}", e))
    }

    suspend fun invalidate(): Result<Unit> = try {
        sendRpcRequest("invalidate", JsonArray(emptyList()))
        Result.Success(Unit)
    } catch (e: Exception) {
        Result.Error(DatabaseException("Failed to invalidate session: ${e.message}", e))
    }

    // Variable Management
    suspend fun let(name: String, value: JsonElement): Result<Unit> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(name))
            add(value)
        }
        sendRpcRequest("let", paramsJson)
        Result.Success(Unit)
    } catch (e: Exception) {
        Result.Error(DatabaseException("Failed to set variable '$name': ${e.message}", e))
    }

    suspend fun unset(name: String): Result<Unit> = try {
        val paramsJson = buildJsonArray { add(JsonPrimitive(name)) }
        sendRpcRequest("unset", paramsJson)
        Result.Success(Unit)
    } catch (e: Exception) {
        Result.Error(DatabaseException("Failed to unset variable '$name': ${e.message}", e))
    }

    // Live Queries
    suspend fun <T : Any> live(
        table: String,
        diff: Boolean,
        type: KClass<T>,
        callback: (Diff<T>) -> Unit
    ): Result<String> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(table))
            add(JsonPrimitive(diff))
        }
        val response = sendRpcRequest("live", paramsJson)
        val queryUuid = response.jsonPrimitive.content
        registerLiveQueryCallback(queryUuid) { jsonData ->
            val diffData = if (diff) {
                val operation = jsonData.jsonObject["operation"]?.jsonPrimitive?.content ?: "update"
                val path = jsonData.jsonObject["path"]?.jsonPrimitive?.content
                val value = jsonData.jsonObject["value"]?.let { json.decodeFromJsonElement(type.serializer(), it) }
                Diff(operation, path, value)
            } else {
                val userData = jsonData.takeIf { it !is JsonNull } ?: return@registerLiveQueryCallback
                Diff("update", null, json.decodeFromJsonElement(type.serializer(), userData))
            }
            callback(diffData)
        }
        Result.Success(queryUuid)
    } catch (e: Exception) {
        Result.Error(QueryException("Failed to start live query: ${e.message}", e))
    }

    suspend fun <T : Any> live(
        table: String,
        diff: Boolean = false,
        type: KClass<T>
    ): Result<Flow<Diff<T>>> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(table))
            add(JsonPrimitive(diff))
        }
        val response = sendRpcRequest("live", paramsJson)
        val queryUuid = response.jsonPrimitive.content
        Result.Success(callbackFlow {
            registerLiveQueryCallback(queryUuid) { jsonData ->
                log("Raw live data: $jsonData")
                val diffData = if (diff) {
                    val operation = jsonData.jsonObject["operation"]?.jsonPrimitive?.content ?: "update"
                    val path = jsonData.jsonObject["path"]?.jsonPrimitive?.content
                    val value = jsonData.jsonObject["value"]?.let { json.decodeFromJsonElement(type.serializer(), it) }
                    Diff(operation, path, value)
                } else {
                    val userData = jsonData.takeIf { it !is JsonNull } ?: return@registerLiveQueryCallback
                    Diff("update", null, json.decodeFromJsonElement(type.serializer(), userData))
                }
                trySend(diffData).onFailure { throwable ->
                    log("Failed to send diffData: $throwable")
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

    suspend inline fun <reified T : Any> live(
        table: String,
        diff: Boolean,
        crossinline callback: (Diff<T>) -> Unit
    ): Result<String> = live(table, diff, T::class) { callback(it) }

    suspend fun kill(queryUuid: String): Result<Unit> = try {
        val paramsJson = buildJsonArray { add(JsonPrimitive(queryUuid)) }
        sendRpcRequest("kill", paramsJson)
        unregisterLiveQueryCallback(queryUuid)
        Result.Success(Unit)
    } catch (e: Exception) {
        Result.Error(QueryException("Failed to kill live query: ${e.message}", e))
    }

    // Queries
    suspend fun <T : Any> query(
        sql: String,
        vars: Map<String, JsonElement>? = null,
        type: KClass<T>
    ): Result<List<T>> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(sql))
            add(JsonObject(vars ?: emptyMap()))
        }
        val result = sendRpcRequest("query", paramsJson)
        if (result !is JsonArray) throw QueryException("Expected array, got $result")
        val listSerializer = ListSerializer(type.serializer())
        val results = result.mapNotNull { it.jsonObject }.map { response ->
            val status = response["status"]?.jsonPrimitive?.content
            val resultData = response["result"]
            if (status == "ERR") {
                val errorMessage = resultData?.jsonPrimitive?.content ?: "Unknown error"
                throw QueryException("Query execution failed: $errorMessage")
            }
            // Handle null result for Unit type queries (e.g., DEFINE FUNCTION)
            if (type == Unit::class && resultData is JsonNull) null else resultData
        }.map {
            if (it == null && type == Unit::class) emptyList() // Return empty list for Unit success
            else json.decodeFromJsonElement(listSerializer, it ?: throw QueryException("Unexpected null result for non-Unit type"))
        }.flatten()
        Result.Success(results)
    } catch (e: Exception) {
        Result.Error(QueryException("Query failed: $sql with vars $vars", e))
    }

    suspend inline fun <reified T : Any> query(
        sql: String,
        vars: Map<String, JsonElement>? = null
    ): Result<List<T>> = query(sql, vars, T::class)

    suspend fun queryRaw(
        sql: String,
        vars: Map<String, JsonElement>? = null
    ): Result<List<JsonElement>> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(sql))
            add(JsonObject(vars ?: emptyMap()))
        }
        val result = sendRpcRequest("query", paramsJson)
        if (result !is JsonArray) throw QueryException("Expected array, got $result")
        val rawResults = result.mapNotNull { it.jsonObject }.flatMap { response ->
            val status = response["status"]?.jsonPrimitive?.content
            val resultData = response["result"]
            if (status == "ERR") {
                val errorMessage = resultData?.jsonPrimitive?.content ?: "Unknown error"
                throw QueryException("Raw query execution failed: $errorMessage")
            }
            when (resultData) {
                is JsonArray -> resultData.toList()
                else -> listOf(resultData)
            }
        }.filterNotNull()
        Result.Success(rawResults)
    } catch (e: Exception) {
        Result.Error(QueryException("Raw query failed: $sql with vars $vars", e))
    }

    suspend fun <T : Any> graphql(
        query: String,
        options: Map<String, JsonElement>? = null,
        type: KClass<T>
    ): Result<T> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(query))
            add(options?.let { JsonObject(it) } ?: JsonNull)
        }
        val result = sendRpcRequest("graphql", paramsJson)
        Result.Success(json.decodeFromJsonElement(type.serializer(), result))
    } catch (e: Exception) {
        Result.Error(QueryException("Failed to execute GraphQL query '$query': ${e.message}", e))
    }

    suspend inline fun <reified T : Any> graphql(
        query: String,
        options: Map<String, JsonElement>? = null
    ): Result<T> = graphql(query, options, T::class)

    suspend fun <T : Any> run(
        funcName: String,
        version: String? = null,
        args: List<JsonElement>,
        type: KClass<T>
    ): Result<T> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(funcName))
            add(version?.let { JsonPrimitive(it) } ?: JsonNull)
            add(JsonArray(args))
        }
        val result = sendRpcRequest("run", paramsJson)
        Result.Success(json.decodeFromJsonElement(type.serializer(), result))
    } catch (e: Exception) {
        Result.Error(QueryException("Failed to run function '$funcName': ${e.message}", e))
    }

    suspend inline fun <reified T : Any> run(
        funcName: String,
        version: String? = null,
        args: List<JsonElement>
    ): Result<T> = run(funcName, version, args, T::class)

    // Data Manipulation
    suspend fun <T : Any> select(thing: RecordId, type: KClass<T>): Result<List<T>> = try {
        val paramsJson = buildJsonArray { add(JsonPrimitive(thing.toString())) }
        val result = sendRpcRequest("select", paramsJson)
        val listSerializer = ListSerializer(type.serializer())
        val records = when (result) {
            is JsonNull -> emptyList()
            is JsonObject -> listOf(json.decodeFromJsonElement(type.serializer(), result))
            is JsonArray -> json.decodeFromJsonElement(listSerializer, result)
            else -> throw DatabaseException("Unexpected response format for select: $result")
        }
        Result.Success(records)
    } catch (e: Exception) {
        Result.Error(OperationException("Select failed on $thing", e))
    }

    suspend inline fun <reified T : Any> select(thing: RecordId): Result<List<T>> = select(thing, T::class)

    suspend fun <T : Any> create(thing: RecordId, data: T? = null, type: KClass<T>): Result<T> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(thing.toString()))
            add(data?.let { json.encodeToJsonElement(type.serializer(), it) } ?: JsonNull)
        }
        val result = sendRpcRequest("create", paramsJson)
        val created = when (result) {
            is JsonArray -> json.decodeFromJsonElement(type.serializer(), result.first())
            is JsonObject -> json.decodeFromJsonElement(type.serializer(), result)
            else -> throw DatabaseException("Unexpected response format for create: $result")
        }
        Result.Success(created)
    } catch (e: Exception) {
        Result.Error(OperationException("Create failed on $thing", e))
    }

    suspend inline fun <reified T : Any> create(thing: RecordId, data: T? = null): Result<T> = create(thing, data, T::class)

    suspend fun <T : Any> insert(table: String, data: List<T>, type: KClass<T>): Result<List<T>> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(table))
            add(JsonArray(data.map { json.encodeToJsonElement(type.serializer(), it) }))
        }
        val result = sendRpcRequest("insert", paramsJson)
        Result.Success(json.decodeFromJsonElement(ListSerializer(type.serializer()), result))
    } catch (e: Exception) {
        Result.Error(OperationException("Insert failed into $table", e))
    }

    suspend inline fun <reified T : Any> insert(table: String, data: List<T>): Result<List<T>> = insert(table, data, T::class)

    suspend fun <T : Any> insertRelation(table: String, data: T, type: KClass<T>): Result<T> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(table))
            add(json.encodeToJsonElement(type.serializer(), data))
        }
        val result = sendRpcRequest("insert_relation", paramsJson)
        Result.Success(json.decodeFromJsonElement(type.serializer(), result.jsonArray.first()))
    } catch (e: Exception) {
        Result.Error(OperationException("Insert relation failed into $table", e))
    }

    suspend inline fun <reified T : Any> insertRelation(table: String, data: T): Result<T> = insertRelation(table, data, T::class)

    suspend fun <T : Any> update(thing: RecordId, data: T, type: KClass<T>): Result<T> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(thing.toString()))
            add(json.encodeToJsonElement(type.serializer(), data))
        }
        val result = sendRpcRequest("update", paramsJson)
        val updated = when (result) {
            is JsonNull -> throw DatabaseException("No record found to update for $thing")
            is JsonObject -> json.decodeFromJsonElement(type.serializer(), result)
            is JsonArray -> json.decodeFromJsonElement(type.serializer(), result.first())
            else -> throw DatabaseException("Unexpected response format for update: $result")
        }
        Result.Success(updated)
    } catch (e: Exception) {
        Result.Error(OperationException("Update failed on $thing", e))
    }

    suspend inline fun <reified T : Any> update(thing: RecordId, data: T): Result<T> = update(thing, data, T::class)

    suspend fun <T : Any> upsert(thing: RecordId, data: T, type: KClass<T>): Result<T> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(thing.toString()))
            add(json.encodeToJsonElement(type.serializer(), data))
        }
        val result = sendRpcRequest("upsert", paramsJson)
        Result.Success(json.decodeFromJsonElement(type.serializer(), result))
    } catch (e: Exception) {
        Result.Error(OperationException("Upsert failed on $thing", e))
    }

    suspend inline fun <reified T : Any> upsert(thing: RecordId, data: T): Result<T> = upsert(thing, data, T::class)

    suspend fun <T : Any> relate(
        inThing: RecordId,
        relation: String,
        outThing: RecordId,
        data: T? = null,
        type: KClass<T>
    ): Result<T> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(inThing.toString()))
            add(JsonPrimitive(relation))
            add(JsonPrimitive(outThing.toString()))
            add(data?.let { json.encodeToJsonElement(type.serializer(), it) } ?: JsonNull)
        }
        val result = sendRpcRequest("relate", paramsJson)
        val relationData = when (result) {
            is JsonNull -> throw DatabaseException("No relation created for $inThing to $outThing")
            is JsonObject -> json.decodeFromJsonElement(type.serializer(), result)
            is JsonArray -> json.decodeFromJsonElement(type.serializer(), result.first())
            else -> throw DatabaseException("Unexpected response format for relate: $result")
        }
        Result.Success(relationData)
    } catch (e: Exception) {
        Result.Error(OperationException("Relate failed from $inThing to $outThing", e))
    }

    suspend inline fun <reified T : Any> relate(
        inThing: RecordId,
        relation: String,
        outThing: RecordId,
        data: T? = null
    ): Result<T> = relate(inThing, relation, outThing, data, T::class)

    suspend fun <T : Any> merge(thing: RecordId, data: T, type: KClass<T>): Result<T> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(thing.toString()))
            add(json.encodeToJsonElement(type.serializer(), data))
        }
        val result = sendRpcRequest("merge", paramsJson)
        val merged = when (result) {
            is JsonNull -> throw DatabaseException("No record found to merge for $thing")
            is JsonObject -> json.decodeFromJsonElement(type.serializer(), result)
            is JsonArray -> json.decodeFromJsonElement(type.serializer(), result.first())
            else -> throw DatabaseException("Unexpected response format for merge: $result")
        }
        Result.Success(merged)
    } catch (e: Exception) {
        Result.Error(OperationException("Merge failed on $thing", e))
    }

    suspend inline fun <reified T : Any> merge(thing: RecordId, data: T): Result<T> = merge(thing, data, T::class)

    suspend fun <T : Any> patch(
        thing: RecordId,
        patches: List<Patch>,
        diff: Boolean,
        type: KClass<T>
    ): Result<List<T>> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(thing.toString()))
            add(JsonArray(patches.map { json.encodeToJsonElement(it) }))
            add(JsonPrimitive(diff))
        }
        val result = sendRpcRequest("patch", paramsJson)
        val listSerializer = ListSerializer(type.serializer())
        val patched = when (result) {
            is JsonNull -> emptyList()
            is JsonObject -> listOf(json.decodeFromJsonElement(type.serializer(), result))
            is JsonArray -> json.decodeFromJsonElement(listSerializer, result)
            else -> throw DatabaseException("Unexpected response format for patch: $result")
        }
        Result.Success(patched)
    } catch (e: Exception) {
        Result.Error(OperationException("Patch failed on $thing", e))
    }

    suspend inline fun <reified T : Any> patch(
        thing: RecordId,
        patches: List<Patch>,
        diff: Boolean
    ): Result<List<T>> = patch(thing, patches, diff, T::class)

    suspend fun <T : Any> delete(thing: RecordId, type: KClass<T>): Result<List<T>> = try {
        val paramsJson = buildJsonArray { add(JsonPrimitive(thing.toString())) }
        val result = sendRpcRequest("delete", paramsJson)
        val listSerializer = ListSerializer(type.serializer())
        val deleted = when (result) {
            is JsonNull -> emptyList()
            is JsonObject -> listOf(json.decodeFromJsonElement(type.serializer(), result))
            is JsonArray -> json.decodeFromJsonElement(listSerializer, result)
            else -> throw DatabaseException("Unexpected response format for delete: $result")
        }
        Result.Success(deleted)
    } catch (e: Exception) {
        Result.Error(OperationException("Delete failed on $thing", e))
    }

    suspend inline fun <reified T : Any> delete(thing: RecordId): Result<List<T>> = delete(thing, T::class)
}

/**
 * Represents the result of an operation, either success or failure.
 */
sealed class Result<out T> {
    data class Success<T>(val data: T) : Result<T>()
    data class Error<T>(val exception: DatabaseException) : Result<T>()

    inline fun <R> map(transform: (T) -> R): Result<R> = when (this) {
        is Success -> Success(transform(data))
        is Error -> Error(exception)
    }

    inline fun onSuccess(block: (T) -> Unit): Result<T> {
        if (this is Success) block(data)
        return this
    }
}

/**
 * Utility to extract Result value or throw.
 */
fun <T> Result<T>.getOrThrow(): T = when (this) {
    is Result.Success -> data
    is Result.Error -> throw exception
}

/**
 * Base class for database-related exceptions.
 */
open class DatabaseException(message: String, cause: Throwable? = null) : Exception(message, cause)

/**
 * Exception for query-related errors.
 */
class QueryException(message: String, cause: Throwable? = null) : DatabaseException(message, cause)

/**
 * Exception for authentication-related errors.
 */
class AuthenticationException(message: String, cause: Throwable? = null) : DatabaseException(message, cause)

/**
 * Exception for data manipulation operation errors (e.g., select, create, update).
 */
class OperationException(message: String, cause: Throwable? = null) : DatabaseException(message, cause)

/**
 * Represents a SurrealDB record identifier (table:id).
 */
@Serializable(with = RecordIdSerializer::class)
data class RecordId(val table: String, val id: String) {
    constructor(table: String, id: JsonElement) : this(
        table,
        when (id) {
            is JsonPrimitive -> id.content
            else -> id.toString()
        }
    )

    override fun toString(): String = "$table:$id"

    companion object {
        fun parse(thing: String): RecordId {
            val parts = thing.split(":", limit = 2)
            require(parts.size == 2) { "Invalid record ID: $thing (expected table:id)" }
            return RecordId(parts[0], parts[1])
        }
    }
}

object RecordIdSerializer : KSerializer<RecordId> {
    override val descriptor = PrimitiveSerialDescriptor("RecordId", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: RecordId) {
        encoder.encodeString(value.toString())
    }

    override fun deserialize(decoder: Decoder): RecordId {
        return RecordId.parse(decoder.decodeString())
    }
}

/**
 * Represents a diff operation for live queries.
 */
@Serializable
data class Diff<T>(
    val operation: String,
    val path: String?,
    val value: T?
)

@Serializable
sealed class SignInParams {
    @Serializable
    data class Root(val user: String, val pass: String) : SignInParams()

    @Serializable
    data class Namespace(val NS: String, val user: String, val pass: String) : SignInParams()

    @Serializable
    data class Database(val NS: String, val DB: String, val user: String, val pass: String) : SignInParams()

    @Serializable
    data class Record(
        val NS: String,
        val DB: String,
        val AC: String,
        val username: String,
        val password: String,
        val additionalVars: Map<String, JsonElement>? = null
    ) : SignInParams()
}

@Serializable
data class SignUpParams(
    val NS: String,
    val DB: String,
    val AC: String,
    val username: String,
    val password: String,
    val additionalVars: Map<String, JsonElement>? = null
)

/**
 * Represents a patch operation for modifying records.
 */
@Serializable
data class Patch(
    val op: String,
    val path: String,
    val value: JsonElement?
)