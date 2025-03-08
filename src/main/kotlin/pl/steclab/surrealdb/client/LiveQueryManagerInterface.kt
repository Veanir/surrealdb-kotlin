package pl.steclab.surrealdb.client

import kotlinx.coroutines.flow.Flow
import pl.steclab.surrealdb.model.Diff
import pl.steclab.surrealdb.result.Result
import kotlin.reflect.KClass

interface LiveQueryManagerInterface {
    suspend fun <T : Any> live(table: String, diff: Boolean, type: KClass<T>, callback: (Diff<T>) -> Unit): Result<String>
    suspend fun <T : Any> live(table: String, diff: Boolean = false, type: KClass<T>): Result<Flow<Diff<T>>>
    suspend fun kill(queryUuid: String): Result<Unit>
}