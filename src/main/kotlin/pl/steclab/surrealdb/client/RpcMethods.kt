package pl.steclab.surrealdb.client

import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.json.*
import kotlinx.serialization.serializer
import pl.steclab.surrealdb.model.Patch
import pl.steclab.surrealdb.model.RecordId
import pl.steclab.surrealdb.model.SignInParams
import pl.steclab.surrealdb.model.SignUpParams
import pl.steclab.surrealdb.result.*
import kotlin.reflect.KClass

@OptIn(InternalSerializationApi::class)
class RpcMethods(private val client: SurrealDBClient) : RpcMethodsInterface {
    override suspend fun use(namespace: String?, database: String?): Result<JsonElement> = try {
        val paramsJson = buildJsonArray {
            add(namespace?.let { JsonPrimitive(it) } ?: JsonNull)
            add(database?.let { JsonPrimitive(it) } ?: JsonNull)
        }
        Result.Success(client.sendRpcRequest("use", paramsJson))
    } catch (e: Exception) {
        Result.Error(DatabaseException("Failed to set namespace/database: ${e.message}", e))
    }

    override suspend fun info(): Result<JsonElement> = try {
        Result.Success(client.sendRpcRequest("info", JsonArray(emptyList())))
    } catch (e: Exception) {
        Result.Error(DatabaseException("Failed to retrieve info: ${e.message}", e))
    }

    override suspend fun version(): Result<JsonElement> = try {
        Result.Success(client.sendRpcRequest("version", JsonArray(emptyList())))
    } catch (e: Exception) {
        Result.Error(DatabaseException("Failed to retrieve version: ${e.message}", e))
    }

    override suspend fun signup(params: SignUpParams): Result<String?> = try {
        val paramsJson = buildJsonArray { add(client.json.encodeToJsonElement(params)) }
        val result = client.sendRpcRequest("signup", paramsJson)
        Result.Success(if (result is JsonNull) null else result.jsonPrimitive.content)
    } catch (e: Exception) {
        Result.Error(AuthenticationException("Signup failed: ${e.message}", e))
    }

    override suspend fun signin(params: SignInParams): Result<String?> = try {
        val paramsJson = buildJsonArray { add(client.json.encodeToJsonElement(params)) }
        val result = client.sendRpcRequest("signin", paramsJson)
        Result.Success(if (result is JsonNull) null else result.jsonPrimitive.content)
    } catch (e: Exception) {
        Result.Error(AuthenticationException("Signin failed: ${e.message}", e))
    }

    override suspend fun authenticate(token: String): Result<Unit> = try {
        val paramsJson = buildJsonArray { add(JsonPrimitive(token)) }
        client.sendRpcRequest("authenticate", paramsJson)
        Result.Success(Unit)
    } catch (e: Exception) {
        Result.Error(AuthenticationException("Authentication failed: ${e.message}", e))
    }

    override suspend fun invalidate(): Result<Unit> = try {
        client.sendRpcRequest("invalidate", JsonArray(emptyList()))
        Result.Success(Unit)
    } catch (e: Exception) {
        Result.Error(DatabaseException("Failed to invalidate session: ${e.message}", e))
    }

    override suspend fun let(name: String, value: JsonElement): Result<Unit> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(name))
            add(value)
        }
        client.sendRpcRequest("let", paramsJson)
        Result.Success(Unit)
    } catch (e: Exception) {
        Result.Error(DatabaseException("Failed to set variable '$name': ${e.message}", e))
    }

    override suspend fun unset(name: String): Result<Unit> = try {
        val paramsJson = buildJsonArray { add(JsonPrimitive(name)) }
        client.sendRpcRequest("unset", paramsJson)
        Result.Success(Unit)
    } catch (e: Exception) {
        Result.Error(DatabaseException("Failed to unset variable '$name': ${e.message}", e))
    }

    override suspend fun <T : Any> query(
        sql: String,
        vars: Map<String, JsonElement>?,
        type: KClass<T>
    ): Result<List<T>> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(sql))
            add(JsonObject(vars ?: emptyMap()))
        }
        val result = client.sendRpcRequest("query", paramsJson)
        if (result !is JsonArray) throw QueryException("Expected array, got $result")
        val listSerializer = ListSerializer(type.serializer())
        val results = result.mapNotNull { it.jsonObject }.map { response ->
            val status = response["status"]?.jsonPrimitive?.content
            val resultData = response["result"]
            if (status == "ERR") {
                val errorMessage = resultData?.jsonPrimitive?.content ?: "Unknown error"
                throw QueryException("Query execution failed: $errorMessage")
            }
            if (type == Unit::class && resultData is JsonNull) null else resultData
        }.map {
            if (it == null && type == Unit::class) emptyList()
            else client.json.decodeFromJsonElement(listSerializer, it ?: throw QueryException("Unexpected null result for non-Unit type"))
        }.flatten()
        Result.Success(results)
    } catch (e: Exception) {
        Result.Error(QueryException("Query failed: $sql with vars $vars", e))
    }

    suspend inline fun <reified T : Any> query(
        sql: String,
        vars: Map<String, JsonElement>? = null
    ): Result<List<T>> = query(sql, vars, T::class)

    override suspend fun queryRaw(
        sql: String,
        vars: Map<String, JsonElement>?
    ): Result<List<JsonElement>> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(sql))
            add(JsonObject(vars ?: emptyMap()))
        }
        val result = client.sendRpcRequest("query", paramsJson)
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

    override suspend fun <T : Any> graphql(
        query: String,
        options: Map<String, JsonElement>?,
        type: KClass<T>
    ): Result<T> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(query))
            add(options?.let { JsonObject(it) } ?: JsonNull)
        }
        val result = client.sendRpcRequest("graphql", paramsJson)
        Result.Success(client.json.decodeFromJsonElement(type.serializer(), result))
    } catch (e: Exception) {
        Result.Error(QueryException("Failed to execute GraphQL query '$query': ${e.message}", e))
    }

    suspend inline fun <reified T : Any> graphql(
        query: String,
        options: Map<String, JsonElement>? = null
    ): Result<T> = graphql(query, options, T::class)

    override suspend fun <T : Any> run(
        funcName: String,
        version: String?,
        args: List<JsonElement>,
        type: KClass<T>
    ): Result<T> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(funcName))
            add(version?.let { JsonPrimitive(it) } ?: JsonNull)
            add(JsonArray(args))
        }
        val result = client.sendRpcRequest("run", paramsJson)
        Result.Success(client.json.decodeFromJsonElement(type.serializer(), result))
    } catch (e: Exception) {
        Result.Error(QueryException("Failed to run function '$funcName': ${e.message}", e))
    }

    suspend inline fun <reified T : Any> run(
        funcName: String,
        version: String? = null,
        args: List<JsonElement>
    ): Result<T> = run(funcName, version, args, T::class)

    override suspend fun <T : Any> select(thing: RecordId, type: KClass<T>): Result<List<T>> = try {
        val paramsJson = buildJsonArray { add(JsonPrimitive(thing.toString())) }
        val result = client.sendRpcRequest("select", paramsJson)
        val listSerializer = ListSerializer(type.serializer())
        val records = when (result) {
            is JsonNull -> emptyList()
            is JsonObject -> listOf(client.json.decodeFromJsonElement(type.serializer(), result))
            is JsonArray -> client.json.decodeFromJsonElement(listSerializer, result)
            else -> throw DatabaseException("Unexpected response format for select: $result")
        }
        Result.Success(records)
    } catch (e: Exception) {
        Result.Error(OperationException("Select failed on $thing", e))
    }

    suspend inline fun <reified T : Any> select(thing: RecordId): Result<List<T>> = select(thing, T::class)

    override suspend fun <T : Any> create(thing: RecordId, data: T?, type: KClass<T>): Result<T> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(thing.toString()))
            add(data?.let { client.json.encodeToJsonElement(type.serializer(), it) } ?: JsonNull)
        }
        val result = client.sendRpcRequest("create", paramsJson)
        val created = when (result) {
            is JsonArray -> client.json.decodeFromJsonElement(type.serializer(), result.first())
            is JsonObject -> client.json.decodeFromJsonElement(type.serializer(), result)
            else -> throw DatabaseException("Unexpected response format for create: $result")
        }
        Result.Success(created)
    } catch (e: Exception) {
        Result.Error(OperationException("Create failed on $thing", e))
    }

    suspend inline fun <reified T : Any> create(thing: RecordId, data: T? = null): Result<T> = create(thing, data, T::class)

    override suspend fun <T : Any> insert(table: String, data: List<T>, type: KClass<T>): Result<List<T>> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(table))
            add(JsonArray(data.map { client.json.encodeToJsonElement(type.serializer(), it) }))
        }
        val result = client.sendRpcRequest("insert", paramsJson)
        Result.Success(client.json.decodeFromJsonElement(ListSerializer(type.serializer()), result))
    } catch (e: Exception) {
        Result.Error(OperationException("Insert failed into $table", e))
    }

    suspend inline fun <reified T : Any> insert(table: String, data: List<T>): Result<List<T>> = insert(table, data, T::class)

    override suspend fun <T : Any> insertRelation(table: String, data: T, type: KClass<T>): Result<T> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(table))
            add(client.json.encodeToJsonElement(type.serializer(), data))
        }
        val result = client.sendRpcRequest("insert_relation", paramsJson)
        Result.Success(client.json.decodeFromJsonElement(type.serializer(), result.jsonArray.first()))
    } catch (e: Exception) {
        Result.Error(OperationException("Insert relation failed into $table", e))
    }

    suspend inline fun <reified T : Any> insertRelation(table: String, data: T): Result<T> = insertRelation(table, data, T::class)

    override suspend fun <T : Any> update(thing: RecordId, data: T, type: KClass<T>): Result<T> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(thing.toString()))
            add(client.json.encodeToJsonElement(type.serializer(), data))
        }
        val result = client.sendRpcRequest("update", paramsJson)
        val updated = when (result) {
            is JsonNull -> throw DatabaseException("No record found to update for $thing")
            is JsonObject -> client.json.decodeFromJsonElement(type.serializer(), result)
            is JsonArray -> client.json.decodeFromJsonElement(type.serializer(), result.first())
            else -> throw DatabaseException("Unexpected response format for update: $result")
        }
        Result.Success(updated)
    } catch (e: Exception) {
        Result.Error(OperationException("Update failed on $thing", e))
    }

    suspend inline fun <reified T : Any> update(thing: RecordId, data: T): Result<T> = update(thing, data, T::class)

    override suspend fun <T : Any> upsert(thing: RecordId, data: T, type: KClass<T>): Result<T> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(thing.toString()))
            add(client.json.encodeToJsonElement(type.serializer(), data))
        }
        val result = client.sendRpcRequest("upsert", paramsJson)
        Result.Success(client.json.decodeFromJsonElement(type.serializer(), result))
    } catch (e: Exception) {
        Result.Error(OperationException("Upsert failed on $thing", e))
    }

    suspend inline fun <reified T : Any> upsert(thing: RecordId, data: T): Result<T> = upsert(thing, data, T::class)

    override suspend fun <T : Any> relate(
        inThing: RecordId,
        relation: String,
        outThing: RecordId,
        data: T?,
        type: KClass<T>
    ): Result<T> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(inThing.toString()))
            add(JsonPrimitive(relation))
            add(JsonPrimitive(outThing.toString()))
            add(data?.let { client.json.encodeToJsonElement(type.serializer(), it) } ?: JsonNull)
        }
        val result = client.sendRpcRequest("relate", paramsJson)
        val relationData = when (result) {
            is JsonNull -> throw DatabaseException("No relation created for $inThing to $outThing")
            is JsonObject -> client.json.decodeFromJsonElement(type.serializer(), result)
            is JsonArray -> client.json.decodeFromJsonElement(type.serializer(), result.first())
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

    override suspend fun <T : Any> merge(thing: RecordId, data: T, type: KClass<T>): Result<T> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(thing.toString()))
            add(client.json.encodeToJsonElement(type.serializer(), data))
        }
        val result = client.sendRpcRequest("merge", paramsJson)
        val merged = when (result) {
            is JsonNull -> throw DatabaseException("No record found to merge for $thing")
            is JsonObject -> client.json.decodeFromJsonElement(type.serializer(), result)
            is JsonArray -> client.json.decodeFromJsonElement(type.serializer(), result.first())
            else -> throw DatabaseException("Unexpected response format for merge: $result")
        }
        Result.Success(merged)
    } catch (e: Exception) {
        Result.Error(OperationException("Merge failed on $thing", e))
    }

    suspend inline fun <reified T : Any> merge(thing: RecordId, data: T): Result<T> = merge(thing, data, T::class)

    override suspend fun <T : Any> patch(
        thing: RecordId,
        patches: List<Patch>,
        diff: Boolean,
        type: KClass<T>
    ): Result<List<T>> = try {
        val paramsJson = buildJsonArray {
            add(JsonPrimitive(thing.toString()))
            add(JsonArray(patches.map { client.json.encodeToJsonElement(it) }))
            add(JsonPrimitive(diff))
        }
        val result = client.sendRpcRequest("patch", paramsJson)
        val listSerializer = ListSerializer(type.serializer())
        val patched = when (result) {
            is JsonNull -> emptyList()
            is JsonObject -> listOf(client.json.decodeFromJsonElement(type.serializer(), result))
            is JsonArray -> client.json.decodeFromJsonElement(listSerializer, result)
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

    override suspend fun <T : Any> delete(thing: RecordId, type: KClass<T>): Result<List<T>> = try {
        val paramsJson = buildJsonArray { add(JsonPrimitive(thing.toString())) }
        val result = client.sendRpcRequest("delete", paramsJson)
        val listSerializer = ListSerializer(type.serializer())
        val deleted = when (result) {
            is JsonNull -> emptyList()
            is JsonObject -> listOf(client.json.decodeFromJsonElement(type.serializer(), result))
            is JsonArray -> client.json.decodeFromJsonElement(listSerializer, result)
            else -> throw DatabaseException("Unexpected response format for delete: $result")
        }
        Result.Success(deleted)
    } catch (e: Exception) {
        Result.Error(OperationException("Delete failed on $thing", e))
    }

    suspend inline fun <reified T : Any> delete(thing: RecordId): Result<List<T>> = delete(thing, T::class)
}