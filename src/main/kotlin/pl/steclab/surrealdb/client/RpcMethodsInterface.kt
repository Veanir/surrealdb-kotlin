package pl.steclab.surrealdb.client

import kotlinx.serialization.json.JsonElement
import pl.steclab.surrealdb.model.Patch
import pl.steclab.surrealdb.model.RecordId
import pl.steclab.surrealdb.model.SignInParams
import pl.steclab.surrealdb.model.SignUpParams
import pl.steclab.surrealdb.result.Result
import kotlin.reflect.KClass

interface RpcMethodsInterface {
    suspend fun use(namespace: String? = null, database: String?): Result<JsonElement>
    suspend fun info(): Result<JsonElement>
    suspend fun version(): Result<JsonElement>
    suspend fun signup(params: SignUpParams): Result<String?>
    suspend fun signin(params: SignInParams): Result<String?>
    suspend fun authenticate(token: String): Result<Unit>
    suspend fun invalidate(): Result<Unit>
    suspend fun let(name: String, value: JsonElement): Result<Unit>
    suspend fun unset(name: String): Result<Unit>
    suspend fun <T : Any> query(sql: String, vars: Map<String, JsonElement>? = null, type: KClass<T>): Result<List<T>>
    suspend fun queryRaw(sql: String, vars: Map<String, JsonElement>? = null): Result<List<JsonElement>>
    suspend fun <T : Any> graphql(query: String, options: Map<String, JsonElement>? = null, type: KClass<T>): Result<T>
    suspend fun <T : Any> run(funcName: String, version: String? = null, args: List<JsonElement>, type: KClass<T>): Result<T>
    suspend fun <T : Any> select(thing: RecordId, type: KClass<T>): Result<List<T>>
    suspend fun <T : Any> create(thing: RecordId, data: T? = null, type: KClass<T>): Result<T>
    suspend fun <T : Any> insert(table: String, data: List<T>, type: KClass<T>): Result<List<T>>
    suspend fun <T : Any> insertRelation(table: String, data: T, type: KClass<T>): Result<T>
    suspend fun <T : Any> update(thing: RecordId, data: T, type: KClass<T>): Result<T>
    suspend fun <T : Any> upsert(thing: RecordId, data: T, type: KClass<T>): Result<T>
    suspend fun <T : Any> relate(inThing: RecordId, relation: String, outThing: RecordId, data: T? = null, type: KClass<T>): Result<T>
    suspend fun <T : Any> merge(thing: RecordId, data: T, type: KClass<T>): Result<T>
    suspend fun <T : Any> patch(thing: RecordId, patches: List<Patch>, diff: Boolean, type: KClass<T>): Result<List<T>>
    suspend fun <T : Any> delete(thing: RecordId, type: KClass<T>): Result<List<T>>
}