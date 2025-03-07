package pl.steclab.surrealdb

import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.websocket.*
import io.ktor.websocket.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.json.*
import kotlinx.serialization.serializer
import java.util.concurrent.atomic.AtomicInteger
import kotlin.reflect.KClass

@OptIn(InternalSerializationApi::class)
internal class SurrealDBClient(private val url: String) : SurrealDB {
    private val client = HttpClient(CIO) { install(WebSockets) }
    private val requestIdCounter = AtomicInteger(0)
    private val pendingRequests = mutableMapOf<Int, CompletableDeferred<JsonElement>>()
    private val liveQueryCallbacks = mutableMapOf<String, (JsonElement) -> Unit>()
    private val sendChannel = Channel<String>(capacity = Channel.UNLIMITED)
    private var webSocketSession: DefaultClientWebSocketSession? = null
    private var connectionJob: Job? = null
    internal val json = Json { ignoreUnknownKeys = true }

    override suspend fun connect() {
        connectionJob = CoroutineScope(Dispatchers.IO).launch {
            try {
                client.webSocket(url) {
                    webSocketSession = this
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
                throw DatabaseException("Failed to connect to SurrealDB at $url: ${e.message}")
            }
        }
        delay(100)
    }

    override suspend fun disconnect() {
        connectionJob?.cancelAndJoin()
        webSocketSession?.close()
        sendChannel.close()
        synchronized(pendingRequests) { pendingRequests.clear() }
        synchronized(liveQueryCallbacks) { liveQueryCallbacks.clear() }
        client.close()
    }

    private fun handleMessage(message: String) {
        try {
            val jsonElement = json.parseToJsonElement(message)
            when {
                jsonElement is JsonObject && jsonElement.containsKey("id") -> {
                    val id = jsonElement["id"]?.jsonPrimitive?.intOrNull ?: return
                    val result = jsonElement["result"] ?: JsonNull
                    val error = jsonElement["error"]
                    val deferred = pendingRequests.remove(id) ?: return
                    if (error != null && error != JsonNull) {
                        val errorMessage = error.jsonObject["message"]?.jsonPrimitive?.content ?: error.toString()
                        deferred.completeExceptionally(DatabaseException("RPC error: $errorMessage"))
                    } else {
                        deferred.complete(result)
                    }
                }
                jsonElement is JsonObject && jsonElement.containsKey("queryUuid") -> {
                    val queryUuid = jsonElement["queryUuid"]?.jsonPrimitive?.content ?: return
                    val data = jsonElement["data"] ?: JsonNull
                    liveQueryCallbacks[queryUuid]?.invoke(data)
                }
            }
        } catch (e: Exception) {
            // Log or ignore malformed messages
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
            throw DatabaseException("Failed to send RPC request '$method': ${e.message}")
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
    override suspend fun use(namespace: String?, database: String?): Result<JsonElement> {
        val paramsJson = buildJsonArray {
            add(namespace?.let { JsonPrimitive(it) } ?: JsonNull)
            add(database?.let { JsonPrimitive(it) } ?: JsonNull)
        }
        return try {
            val result = sendRpcRequest("use", paramsJson)
            Result.Success(result)
        } catch (e: Exception) {
            Result.Error(DatabaseException("Failed to set namespace/database: ${e.message}"))
        }
    }

    override suspend fun info(): Result<JsonElement> {
        return try {
            val result = sendRpcRequest("info", JsonArray(emptyList()))
            Result.Success(result)
        } catch (e: Exception) {
            Result.Error(DatabaseException("Failed to retrieve info: ${e.message}"))
        }
    }

    override suspend fun version(): Result<JsonElement> {
        return try {
            val result = sendRpcRequest("version", JsonArray(emptyList()))
            Result.Success(result)
        } catch (e: Exception) {
            Result.Error(DatabaseException("Failed to retrieve version: ${e.message}"))
        }
    }

    // Authentication
    override suspend fun signup(params: SignUpParams): Result<String?> {
        val paramsJson = buildJsonArray { add(json.encodeToJsonElement(params)) }
        return try {
            val result = sendRpcRequest("signup", paramsJson)
            Result.Success(if (result is JsonNull) null else result.jsonPrimitive.content)
        } catch (e: Exception) {
            Result.Error(AuthenticationException("Signup failed: ${e.message}"))
        }
    }

    override suspend fun signin(params: SignInParams): Result<String?> {
        val paramsJson = buildJsonArray { add(json.encodeToJsonElement(params)) }
        return try {
            val result = sendRpcRequest("signin", paramsJson)
            Result.Success(if (result is JsonNull) null else result.jsonPrimitive.content)
        } catch (e: Exception) {
            Result.Error(AuthenticationException("Signin failed: ${e.message}"))
        }
    }

    override suspend fun authenticate(token: String): Result<Unit> {
        val paramsJson = buildJsonArray { add(JsonPrimitive(token)) }
        return try {
            sendRpcRequest("authenticate", paramsJson)
            Result.Success(Unit)
        } catch (e: Exception) {
            Result.Error(AuthenticationException("Authentication failed: ${e.message}"))
        }
    }

    override suspend fun invalidate(): Result<Unit> {
        return try {
            sendRpcRequest("invalidate", JsonArray(emptyList()))
            Result.Success(Unit)
        } catch (e: Exception) {
            Result.Error(DatabaseException("Failed to invalidate session: ${e.message}"))
        }
    }

    // Variables
    override suspend fun let(name: String, value: JsonElement): Result<Unit> {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(name))
            add(value)
        }
        return try {
            sendRpcRequest("let", paramsJson)
            Result.Success(Unit)
        } catch (e: Exception) {
            Result.Error(DatabaseException("Failed to set variable '$name': ${e.message}"))
        }
    }

    override suspend fun unset(name: String): Result<Unit> {
        val paramsJson = buildJsonArray { add(JsonPrimitive(name)) }
        return try {
            sendRpcRequest("unset", paramsJson)
            Result.Success(Unit)
        } catch (e: Exception) {
            Result.Error(DatabaseException("Failed to unset variable '$name': ${e.message}"))
        }
    }

    // Live Queries
    override suspend fun <T : Any> live(
        table: String,
        diff: Boolean,
        type: KClass<T>,
        callback: (Diff<T>) -> Unit
    ): Result<String> {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(table))
            add(JsonPrimitive(diff))
        }
        return try {
            val response = sendRpcRequest("live", paramsJson)
            val queryUuid = response.jsonPrimitive.content
            registerLiveQueryCallback(queryUuid) { jsonData ->
                if (diff) {
                    val operation = jsonData.jsonObject["operation"]?.jsonPrimitive?.content ?: "update"
                    val path = jsonData.jsonObject["path"]?.jsonPrimitive?.content
                    val value = jsonData.jsonObject["value"]?.let { json.decodeFromJsonElement(type.serializer(), it) }
                    callback(Diff(operation, path, value))
                } else {
                    callback(Diff("update", null, json.decodeFromJsonElement(type.serializer(), jsonData)))
                }
            }
            Result.Success(queryUuid)
        } catch (e: Exception) {
            Result.Error(QueryException("Failed to start live query: ${e.message}"))
        }
    }

    override suspend fun kill(queryUuid: String): Result<Unit> {
        val paramsJson = buildJsonArray { add(JsonPrimitive(queryUuid)) }
        return try {
            sendRpcRequest("kill", paramsJson)
            unregisterLiveQueryCallback(queryUuid)
            Result.Success(Unit)
        } catch (e: Exception) {
            Result.Error(QueryException("Failed to kill live query: ${e.message}"))
        }
    }

    override suspend fun <T : Any> query(
        sql: String,
        vars: Map<String, JsonElement>?,
        type: KClass<T>,
        lenient: Boolean
    ): Result<out Any> {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(sql))
            add(JsonObject(vars ?: emptyMap()))
        }
        return try {
            val result = sendRpcRequest("query", paramsJson)
            if (result !is JsonArray) throw QueryException("Expected array, got $result")
            val listSerializer = ListSerializer(type.serializer())
            val results = result.mapNotNull { it.jsonObject["result"] }
                .map { json.decodeFromJsonElement(listSerializer, it) }
                .flatten()
            Result.Success(results)
        } catch (e: Exception) {
            if (lenient) Result.Success(sendRpcRequest("query", paramsJson))
            else Result.Error(QueryException("Query '$sql' failed: ${e.message}"))
        }
    }

    override suspend fun <T : Any> querySingle(
        sql: String,
        vars: Map<String, JsonElement>?,
        type: KClass<T>,
        lenient: Boolean
    ): Result<out Any> {
        val queryResult = query(sql, vars, type, lenient)
        return when (queryResult) {
            is Result.Success -> {
                when (val result = queryResult.data) {
                    is List<*> -> when (result.size) {
                        0 -> Result.Error(QueryException("Expected one result, got none"))
                        1 -> {
                            val first = result.first()
                            if (first != null) Result.Success(first)
                            else Result.Error(QueryException("First result was null"))
                        }
                        else -> Result.Error(QueryException("Expected one result, got ${result.size}"))
                    }
                    is JsonElement -> Result.Success(result) // Lenient mode
                    else -> Result.Error(QueryException("Unexpected result type: $result"))
                }
            }
            is Result.Error -> queryResult // Propagate the error
        }
    }

    override suspend fun <T : Any> graphql(
        query: String,
        options: Map<String, JsonElement>?,
        type: KClass<T>
    ): Result<T> {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(query))
            add(options?.let { JsonObject(it) } ?: JsonNull)
        }
        return try {
            val result = sendRpcRequest("graphql", paramsJson)
            Result.Success(json.decodeFromJsonElement(type.serializer(), result))
        } catch (e: Exception) {
            Result.Error(QueryException("Failed to execute GraphQL query '$query': ${e.message}"))
        }
    }

    override suspend fun <T : Any> run(
        funcName: String,
        version: String?,
        args: List<JsonElement>,
        type: KClass<T>
    ): Result<T> {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(funcName))
            add(version?.let { JsonPrimitive(it) } ?: JsonNull)
            add(JsonArray(args))
        }
        return try {
            val result = sendRpcRequest("run", paramsJson)
            Result.Success(json.decodeFromJsonElement(type.serializer(), result))
        } catch (e: Exception) {
            Result.Error(QueryException("Failed to run function '$funcName': ${e.message}"))
        }
    }

    // Data Manipulation
    override suspend fun <T : Any> select(thing: RecordId, type: KClass<T>): Result<List<T>> {
        val paramsJson = buildJsonArray { add(JsonPrimitive(thing.toString())) }
        return try {
            val result = sendRpcRequest("select", paramsJson)
            val listSerializer = ListSerializer(type.serializer())
            Result.Success(if (result is JsonNull) emptyList() else json.decodeFromJsonElement(listSerializer, result))
        } catch (e: Exception) {
            Result.Error(QueryException("Failed to select from $thing: ${e.message}"))
        }
    }

    override suspend fun <T : Any> create(thing: RecordId, data: T?, type: KClass<T>): Result<T> {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(thing.toString()))
            add(data?.let { json.encodeToJsonElement(type.serializer(), it) } ?: JsonNull)
        }
        return try {
            val result = sendRpcRequest("create", paramsJson)
            val record = json.decodeFromJsonElement(type.serializer(), if (result is JsonArray) result.first() else result)
            Result.Success(record)
        } catch (e: Exception) {
            Result.Error(QueryException("Failed to create $thing: ${e.message}"))
        }
    }

    override suspend fun <T : Any> insert(thing: RecordId, data: List<T>, type: KClass<T>): Result<List<T>> {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(thing.toString()))
            add(JsonArray(data.map { json.encodeToJsonElement(type.serializer(), it) }))
        }
        return try {
            val result = sendRpcRequest("insert", paramsJson)
            Result.Success(json.decodeFromJsonElement(ListSerializer(type.serializer()), result))
        } catch (e: Exception) {
            Result.Error(QueryException("Failed to insert into $thing: ${e.message}"))
        }
    }

    override suspend fun <T : Any> insertRelation(table: RecordId, data: T, type: KClass<T>): Result<T> {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(table.toString()))
            add(json.encodeToJsonElement(type.serializer(), data))
        }
        return try {
            val result = sendRpcRequest("insert_relation", paramsJson)
            Result.Success(json.decodeFromJsonElement(type.serializer(), result.jsonArray.first()))
        } catch (e: Exception) {
            Result.Error(QueryException("Failed to insert relation into $table: ${e.message}"))
        }
    }

    override suspend fun <T : Any> update(thing: RecordId, data: T, type: KClass<T>): Result<T> {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(thing.toString()))
            add(json.encodeToJsonElement(type.serializer(), data))
        }
        return try {
            val result = sendRpcRequest("update", paramsJson)
            Result.Success(json.decodeFromJsonElement(type.serializer(), result.jsonArray.first()))
        } catch (e: Exception) {
            Result.Error(QueryException("Failed to update $thing: ${e.message}"))
        }
    }

    override suspend fun <T : Any> upsert(thing: RecordId, data: T, type: KClass<T>): Result<T> {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(thing.toString()))
            add(json.encodeToJsonElement(type.serializer(), data))
        }
        return try {
            val result = sendRpcRequest("upsert", paramsJson)
            Result.Success(json.decodeFromJsonElement(type.serializer(), result))
        } catch (e: Exception) {
            Result.Error(QueryException("Failed to upsert $thing: ${e.message}"))
        }
    }

    override suspend fun <T : Any> relate(
        inThing: RecordId,
        relation: String,
        outThing: RecordId,
        data: T?,
        type: KClass<T>
    ): Result<T> {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(inThing.toString()))
            add(JsonPrimitive(relation))
            add(JsonPrimitive(outThing.toString()))
            add(data?.let { json.encodeToJsonElement(type.serializer(), it) } ?: JsonNull)
        }
        return try {
            val result = sendRpcRequest("relate", paramsJson)
            Result.Success(json.decodeFromJsonElement(type.serializer(), result.jsonArray.first()))
        } catch (e: Exception) {
            Result.Error(QueryException("Failed to relate $inThing to $outThing: ${e.message}"))
        }
    }

    override suspend fun <T : Any> merge(thing: RecordId, data: T, type: KClass<T>): Result<T> {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(thing.toString()))
            add(json.encodeToJsonElement(type.serializer(), data))
        }
        return try {
            val result = sendRpcRequest("merge", paramsJson)
            Result.Success(json.decodeFromJsonElement(type.serializer(), result.jsonArray.first()))
        } catch (e: Exception) {
            Result.Error(QueryException("Failed to merge $thing: ${e.message}"))
        }
    }

    override suspend fun <T : Any> patch(
        thing: RecordId,
        patches: List<Patch>,
        diff: Boolean,
        type: KClass<T>
    ): Result<List<T>> {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(thing.toString()))
            add(JsonArray(patches.map { json.encodeToJsonElement(it) }))
            add(JsonPrimitive(diff))
        }
        return try {
            val result = sendRpcRequest("patch", paramsJson)
            Result.Success(json.decodeFromJsonElement(ListSerializer(type.serializer()), result))
        } catch (e: Exception) {
            Result.Error(QueryException("Failed to patch $thing: ${e.message}"))
        }
    }

    override suspend fun <T : Any> delete(thing: RecordId, type: KClass<T>): Result<List<T>> {
        val paramsJson = buildJsonArray { add(JsonPrimitive(thing.toString())) }
        return try {
            val result = sendRpcRequest("delete", paramsJson)
            Result.Success(json.decodeFromJsonElement(ListSerializer(type.serializer()), result))
        } catch (e: Exception) {
            Result.Error(QueryException("Failed to delete $thing: ${e.message}"))
        }
    }
}