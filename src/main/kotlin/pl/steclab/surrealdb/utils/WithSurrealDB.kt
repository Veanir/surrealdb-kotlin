package pl.steclab.surrealdb.utils

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import pl.steclab.surrealdb.client.LiveQueryManager
import pl.steclab.surrealdb.client.RpcMethods
import pl.steclab.surrealdb.client.SurrealDBClient
import pl.steclab.surrealdb.client.SurrealDBClientConfig
import pl.steclab.surrealdb.model.Diff
import pl.steclab.surrealdb.model.Patch
import pl.steclab.surrealdb.model.RecordId
import pl.steclab.surrealdb.model.SignInParams
import pl.steclab.surrealdb.model.SignUpParams
import pl.steclab.surrealdb.result.*
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.json.*
import kotlin.reflect.KClass

@OptIn(InternalSerializationApi::class)
fun <T> withSurrealDB(config: SurrealDBClientConfig, block: suspend SurrealDBOperations.() -> T): T {
    val client = SurrealDBClient(config)
    val operations = SurrealDBOperations(client)
    return runBlocking {
        try {
            client.connect()
            config.credentials?.let { operations.signin(it).getOrThrow() }
            config.namespace?.let { ns -> config.database?.let { db -> operations.use(ns, db).getOrThrow() } }
            operations.block()
        } finally {
            client.disconnect()
        }
    }
}

@OptIn(InternalSerializationApi::class)
class SurrealDBOperations(private val client: SurrealDBClient) {
    private val rpc = RpcMethods(client)
    private val liveQuery = LiveQueryManager(client)

    // Non-generic RPC methods
    suspend fun use(namespace: String? = null, database: String?): Result<JsonElement> = rpc.use(namespace, database)
    suspend fun info(): Result<JsonElement> = rpc.info()
    suspend fun version(): Result<JsonElement> = rpc.version()
    suspend fun signup(params: SignUpParams): Result<String?> = rpc.signup(params)
    suspend fun signin(params: SignInParams): Result<String?> = rpc.signin(params)
    suspend fun authenticate(token: String): Result<Unit> = rpc.authenticate(token)
    suspend fun invalidate(): Result<Unit> = rpc.invalidate()
    suspend fun let(name: String, value: JsonElement): Result<Unit> = rpc.let(name, value)
    suspend fun unset(name: String): Result<Unit> = rpc.unset(name)
    suspend fun queryRaw(sql: String, vars: Map<String, JsonElement>? = null): Result<List<JsonElement>> = rpc.queryRaw(sql, vars)

    // Generic RPC methods with reified overloads
    suspend fun <T : Any> query(sql: String, vars: Map<String, JsonElement>? = null, type: KClass<T>): Result<List<T>> = rpc.query(sql, vars, type)
    suspend inline fun <reified T : Any> query(sql: String, vars: Map<String, JsonElement>? = null): Result<List<T>> = query(sql, vars, T::class)

    suspend fun <T : Any> graphql(query: String, options: Map<String, JsonElement>? = null, type: KClass<T>): Result<T> = rpc.graphql(query, options, type)
    suspend inline fun <reified T : Any> graphql(query: String, options: Map<String, JsonElement>? = null): Result<T> = graphql(query, options, T::class)

    suspend fun <T : Any> run(funcName: String, version: String? = null, args: List<JsonElement>, type: KClass<T>): Result<T> = rpc.run(funcName, version, args, type)
    suspend inline fun <reified T : Any> run(funcName: String, version: String? = null, args: List<JsonElement>): Result<T> = run(funcName, version, args, T::class)

    suspend fun <T : Any> select(thing: RecordId, type: KClass<T>): Result<List<T>> = rpc.select(thing, type)
    suspend inline fun <reified T : Any> select(thing: RecordId): Result<List<T>> = select(thing, T::class)

    suspend fun <T : Any> create(thing: RecordId, data: T? = null, type: KClass<T>): Result<T> = rpc.create(thing, data, type)
    suspend inline fun <reified T : Any> create(thing: RecordId, data: T? = null): Result<T> = create(thing, data, T::class)

    suspend fun <T : Any> insert(table: String, data: List<T>, type: KClass<T>): Result<List<T>> = rpc.insert(table, data, type)
    suspend inline fun <reified T : Any> insert(table: String, data: List<T>): Result<List<T>> = insert(table, data, T::class)

    suspend fun <T : Any> insertRelation(table: String, data: T, type: KClass<T>): Result<T> = rpc.insertRelation(table, data, type)
    suspend inline fun <reified T : Any> insertRelation(table: String, data: T): Result<T> = insertRelation(table, data, T::class)

    suspend fun <T : Any> update(thing: RecordId, data: T, type: KClass<T>): Result<T> = rpc.update(thing, data, type)
    suspend inline fun <reified T : Any> update(thing: RecordId, data: T): Result<T> = update(thing, data, T::class)

    suspend fun <T : Any> upsert(thing: RecordId, data: T, type: KClass<T>): Result<T> = rpc.upsert(thing, data, type)
    suspend inline fun <reified T : Any> upsert(thing: RecordId, data: T): Result<T> = upsert(thing, data, T::class)

    suspend fun <T : Any> relate(inThing: RecordId, relation: String, outThing: RecordId, data: T? = null, type: KClass<T>): Result<T> = rpc.relate(inThing, relation, outThing, data, type)
    suspend inline fun <reified T : Any> relate(inThing: RecordId, relation: String, outThing: RecordId, data: T? = null): Result<T> = relate(inThing, relation, outThing, data, T::class)

    suspend fun <T : Any> merge(thing: RecordId, data: T, type: KClass<T>): Result<T> = rpc.merge(thing, data, type)
    suspend inline fun <reified T : Any> merge(thing: RecordId, data: T): Result<T> = merge(thing, data, T::class)

    suspend fun <T : Any> patch(thing: RecordId, patches: List<Patch>, diff: Boolean, type: KClass<T>): Result<List<T>> = rpc.patch(thing, patches, diff, type)
    suspend inline fun <reified T : Any> patch(thing: RecordId, patches: List<Patch>, diff: Boolean): Result<List<T>> = patch(thing, patches, diff, T::class)

    suspend fun <T : Any> delete(thing: RecordId, type: KClass<T>): Result<List<T>> = rpc.delete(thing, type)
    suspend inline fun <reified T : Any> delete(thing: RecordId): Result<List<T>> = delete(thing, T::class)

    // Live query methods
    suspend fun <T : Any> live(table: String, diff: Boolean, type: KClass<T>, callback: (Diff<T>) -> Unit): Result<String> = liveQuery.live(table, diff, type, callback)
    suspend inline fun <reified T : Any> live(table: String, diff: Boolean, noinline callback: (Diff<T>) -> Unit): Result<String> = live(table, diff, T::class, callback)

    suspend fun <T : Any> live(table: String, diff: Boolean = false, type: KClass<T>): Result<Flow<Diff<T>>> = liveQuery.live(table, diff, type)
    suspend inline fun <reified T : Any> live(table: String, diff: Boolean = false): Result<Flow<Diff<T>>> = live(table, diff, T::class)

    suspend fun kill(queryUuid: String): Result<Unit> = liveQuery.kill(queryUuid)
}