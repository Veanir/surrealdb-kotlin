package pl.steclab.surrealdb.result

open class DatabaseException(message: String, cause: Throwable? = null) : Exception(message, cause)

class QueryException(message: String, cause: Throwable? = null) : DatabaseException(message, cause)

class AuthenticationException(message: String, cause: Throwable? = null) : DatabaseException(message, cause)

class OperationException(message: String, cause: Throwable? = null) : DatabaseException(message, cause)