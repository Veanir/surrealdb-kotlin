package pl.steclab.surrealdb.model

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement

@Serializable
data class Patch(
    val op: String,
    val path: String,
    val value: JsonElement?
)