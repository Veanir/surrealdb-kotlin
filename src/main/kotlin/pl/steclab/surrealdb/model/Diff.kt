package pl.steclab.surrealdb.model

import kotlinx.serialization.Serializable

@Serializable
data class Diff<T>(
    val operation: String,
    val path: String?,
    val value: T?
)