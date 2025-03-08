package pl.steclab.surrealdb.result

sealed class Result<out T> {
    data class Success<T>(val data: T) : Result<T>()
    data class Error<T>(val exception: DatabaseException) : Result<T>()

    inline fun <R> map(transform: (T) -> R): Result<R> = when (this) {
        is Success -> Success(transform(data))
        is Error -> Error(exception)
    }

    inline fun onSuccess(block: (T) -> Unit): Result<T> {
        if (this is Success) block(data)
        return this
    }
}

fun <T> Result<T>.getOrThrow(): T = when (this) {
    is Result.Success -> data
    is Result.Error -> throw exception
}