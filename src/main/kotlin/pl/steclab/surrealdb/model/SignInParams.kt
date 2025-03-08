package pl.steclab.surrealdb.model

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement

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