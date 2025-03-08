package pl.steclab.surrealdb.client

import pl.steclab.surrealdb.model.SignInParams
import kotlin.time.Duration

data class SurrealDBClientConfig(
    var url: String = "ws://localhost:8000/rpc",
    var namespace: String? = null,
    var database: String? = null,
    var credentials: SignInParams? = null,
    var connectionTimeout: Duration? = null,
    var verboseLogging: Boolean = false
)