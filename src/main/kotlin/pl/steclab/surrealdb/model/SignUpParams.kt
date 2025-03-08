package pl.steclab.surrealdb.model

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement

@Serializable
data class SignUpParams(
    val NS: String,
    val DB: String,
    val AC: String,
    val username: String,
    val password: String,
    val additionalVars: Map<String, JsonElement>? = null
)