package pl.steclab.surrealdb

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlin.reflect.KClass

/**
 * The public API for interacting with SurrealDB.
 */
interface SurrealDB {
    suspend fun connect()
    suspend fun disconnect()

    // Session management returns raw JSON for flexibility, as results are server-specific
    suspend fun use(namespace: String? = null, database: String? = null): Result<JsonElement>
    suspend fun info(): Result<JsonElement>
    suspend fun version(): Result<JsonElement>

    // Authentication methods return tokens or errors
    suspend fun signup(params: SignUpParams): Result<String?>
    suspend fun signin(params: SignInParams): Result<String?>
    suspend fun authenticate(token: String): Result<Unit>
    suspend fun invalidate(): Result<Unit>

    // Variable management
    suspend fun let(name: String, value: JsonElement): Result<Unit>
    suspend fun unset(name: String): Result<Unit>

    // Live queries with type-safe diff callbacks
    suspend fun <T : Any> live(
        table: String,
        diff: Boolean,
        type: KClass<T>,
        callback: (Diff<T>) -> Unit
    ): Result<String>
    suspend fun kill(queryUuid: String): Result<Unit>

    // Queries with type-safe, flexible results
    suspend fun <T : Any> query(
        sql: String,
        vars: Map<String, JsonElement>? = null,
        type: KClass<T>,
        lenient: Boolean = false
    ): Result<out Any> // Returns List<T> or JsonElement in lenient mode
    suspend fun <T : Any> querySingle(
        sql: String,
        vars: Map<String, JsonElement>? = null,
        type: KClass<T>,
        lenient: Boolean = false
    ): Result<out Any> // Returns T or JsonElement in lenient mode
    suspend fun <T : Any> graphql(
        query: String,
        options: Map<String, JsonElement>? = null,
        type: KClass<T>
    ): Result<T>
    suspend fun <T : Any> run(
        funcName: String,
        version: String? = null,
        args: List<JsonElement>,
        type: KClass<T>
    ): Result<T>

    // Data manipulation with type-safe records
    suspend fun <T : Any> select(thing: RecordId, type: KClass<T>): Result<List<T>>
    suspend fun <T : Any> create(thing: RecordId, data: T? = null, type: KClass<T>): Result<T>
    suspend fun <T : Any> insert(thing: RecordId, data: List<T>, type: KClass<T>): Result<List<T>>
    suspend fun <T : Any> insertRelation(table: RecordId, data: T, type: KClass<T>): Result<T>
    suspend fun <T : Any> update(thing: RecordId, data: T, type: KClass<T>): Result<T>
    suspend fun <T : Any> upsert(thing: RecordId, data: T, type: KClass<T>): Result<T>
    suspend fun <T : Any> relate(
        inThing: RecordId,
        relation: String,
        outThing: RecordId,
        data: T? = null,
        type: KClass<T>
    ): Result<T>
    suspend fun <T : Any> merge(thing: RecordId, data: T, type: KClass<T>): Result<T>
    suspend fun <T : Any> patch(
        thing: RecordId,
        patches: List<Patch>,
        diff: Boolean,
        type: KClass<T>
    ): Result<List<T>>
    suspend fun <T : Any> delete(thing: RecordId, type: KClass<T>): Result<List<T>>

    companion object {
        // Reified extension functions for type inference and flexibility
        suspend inline fun <reified T : Any> SurrealDB.live(
            table: String,
            diff: Boolean,
            crossinline callback: (Diff<T>) -> Unit
        ): Result<String> = live(table, diff, T::class) { callback(it) }

        suspend inline fun <reified T : Any> SurrealDB.query(
            sql: String,
            vars: Map<String, JsonElement>? = null,
            lenient: Boolean = false
        ): Result<out Any> = query(sql, vars, T::class, lenient)

        suspend inline fun <reified T : Any> SurrealDB.querySingle(
            sql: String,
            vars: Map<String, JsonElement>? = null,
            lenient: Boolean = false
        ): Result<out Any> = querySingle(sql, vars, T::class, lenient)

        suspend inline fun <reified T : Any> SurrealDB.select(
            thing: RecordId
        ): Result<List<T>> = select(thing, T::class)

        suspend inline fun <reified T : Any> SurrealDB.create(
            thing: RecordId,
            data: T? = null
        ): Result<T> = create(thing, data, T::class)

        suspend inline fun <reified T : Any> SurrealDB.create(thing: RecordId): Result<T> =
            create(thing, null, T::class)

        suspend inline fun <reified T : Any> SurrealDB.insert(
            thing: RecordId,
            data: List<T>
        ): Result<List<T>> = insert(thing, data, T::class)

        suspend inline fun <reified T : Any> SurrealDB.insertRelation(
            table: RecordId,
            data: T
        ): Result<T> = insertRelation(table, data, T::class)

        suspend inline fun <reified T : Any> SurrealDB.update(
            thing: RecordId,
            data: T
        ): Result<T> = update(thing, data, T::class)

        suspend inline fun <reified T : Any> SurrealDB.upsert(
            thing: RecordId,
            data: T
        ): Result<T> = upsert(thing, data, T::class)

        suspend inline fun <reified T : Any> SurrealDB.relate(
            inThing: RecordId,
            relation: String,
            outThing: RecordId,
            data: T? = null
        ): Result<T> = relate(inThing, relation, outThing, data, T::class)

        suspend inline fun <reified T : Any> SurrealDB.merge(
            thing: RecordId,
            data: T
        ): Result<T> = merge(thing, data, T::class)

        suspend inline fun <reified T : Any> SurrealDB.patch(
            thing: RecordId,
            patches: List<Patch>,
            diff: Boolean
        ): Result<List<T>> = patch(thing, patches, diff, T::class)

        suspend inline fun <reified T : Any> SurrealDB.delete(
            thing: RecordId
        ): Result<List<T>> = delete(thing, T::class)
    }
}

/**
 * Represents the result of an operation, either success or failure.
 */
sealed class Result<out T> {
    data class Success<T>(val data: T) : Result<T>()
    data class Error<T>(val exception: DatabaseException) : Result<T>()

    // Utility to transform success values
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
 * Base class for database-related exceptions.
 */
open class DatabaseException(message: String) : Exception(message)

/**
 * Specific exception for query errors.
 */
class QueryException(message: String) : DatabaseException(message)

/**
 * Specific exception for authentication errors.
 */
class AuthenticationException(message: String) : DatabaseException(message)

/**
 * Represents a SurrealDB record identifier (table:id).
 */
@Serializable
data class RecordId(val table: String, val id: JsonElement) {
    constructor(table: String, id: String) : this(table, JsonPrimitive(id))
    constructor(table: String, id: Number) : this(table, JsonPrimitive(id))

    override fun toString(): String = when (id) {
        is JsonPrimitive -> "$table:${id.contentOrNull}"
        else -> "$table:${Json.encodeToString(JsonElement.serializer(), id)}"
    }

    companion object {
        fun parse(thing: String): RecordId {
            val parts = thing.split(":", limit = 2)
            require(parts.size == 2) { "Invalid record ID: $thing (expected table:id)" }
            return try {
                RecordId(parts[0], Json.parseToJsonElement(parts[1]))
            } catch (e: Exception) {
                RecordId(parts[0], JsonPrimitive(parts[1]))
            }
        }
    }
}

/**
 * Represents a diff operation for live queries.
 */
@Serializable
data class Diff<T>(
    val operation: String, // "create", "update", "delete"
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
sealed class SignUpParams {
    @Serializable
    data class Root(val user: String, val pass: String) : SignUpParams()

    @Serializable
    data class Namespace(val NS: String, val user: String, val pass: String) : SignUpParams()

    @Serializable
    data class Database(val NS: String, val DB: String, val user: String, val pass: String) : SignUpParams()

    @Serializable
    data class Record(
        val NS: String,
        val DB: String,
        val AC: String,
        val username: String,
        val password: String,
        val additionalVars: Map<String, JsonElement>? = null
    ) : SignUpParams()
}

/**
 * Represents a patch operation for modifying records.
 */
@Serializable
data class Patch(
    val op: String,
    val path: String,
    val value: JsonElement?
)