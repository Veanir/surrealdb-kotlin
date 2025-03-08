# Kotlin SurrealDB Driver

A lightweight, coroutine-based Kotlin driver for [SurrealDB](https://surrealdb.com/), a next-generation multi-model database. This driver provides a simple and idiomatic Kotlin API to interact with SurrealDB over its WebSocket RPC interface, supporting queries, live updates, authentication, and more.

## Features
- **Coroutine Support**: Built with Kotlin coroutines for asynchronous operations.
- **Type-Safe Queries**: Leverages Kotlin’s type system and kotlinx.serialization for safe data handling.
- **Automatic Connection Management**: Use the `withSurrealDB` scope function for seamless connection setup and teardown.
- **Comprehensive API**: Supports CRUD operations, live queries, authentication, and custom function execution.
- **Verbose Logging**: Optional logging for debugging WebSocket interactions.

## Installation
Add the driver to your project using Gradle. This assumes the library is published to a repository like Maven Central or a GitHub Packages repository. (If you haven’t published it yet, adjust the coordinates accordingly.)

### Gradle (Kotlin DSL)
```kotlin
TODO
```

## Usage
The driver is designed to be intuitive and leverages Kotlin’s scoping functions for simplicity. Below are examples of common operations.

### Basic Setup
Configure and connect to SurrealDB using `withSurrealDB`:

```kotlin
import pl.steclab.surrealdb.*

suspend fun main() {
    val config = SurrealDBClientConfig().apply {
        url = "ws://localhost:8000/rpc"
        namespace = "test_namespace"
        database = "test_database"
        credentials = SignInParams.Root("root", "root")
        verboseLogging = true
    }

    withSurrealDB(config) {
        // Perform database operations here
        val version = version().getOrThrow()
        println("Connected to SurrealDB version: $version")
    }
}
```

The `withSurrealDB` function automatically manages the connection lifecycle, connecting at the start and disconnecting when the block completes.

### Creating and Querying Records
Create and retrieve records with type-safe operations:

```kotlin
@Serializable
data class User(val name: String, val age: Int)

withSurrealDB(config) {
    // Create a record
    val user = create(RecordId("user", "alice"), User("Alice", 30)).getOrThrow()
    println("Created: $user")

    // Query records
    val users = query<User>("SELECT * FROM user WHERE age > 25").getOrThrow()
    println("Found users: $users")
}
```

### Updating Records
Modify existing records using `update`, `merge`, or `patch`:

```kotlin
withSurrealDB(config) {
    // Full update
    val updated = update(RecordId("user", "alice"), User("Alice", 31)).getOrThrow()
    println("Updated: $updated")

    // Partial merge
    val merged = merge(RecordId("user", "alice"), User("Alice", 32)).getOrThrow()
    println("Merged: $merged")

    // Patch with specific changes
    val patches = listOf(Patch("replace", "age", JsonPrimitive(33)))
    val patched = patch<User>(RecordId("user", "alice"), patches, diff = false).getOrThrow()
    println("Patched: ${patched[0]}")
}
```

### Live Queries
Subscribe to real-time updates using Kotlin Flows or callbacks:

```kotlin
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking

withSurrealDB(config) {
    // Using Flow
    val flow = live<User>("user").getOrThrow()
    runBlocking {
        val updates = flow.take(3).toList()
        println("Live updates: $updates")
    }

    // Using callback
    val queryUuid = live<User>("user", diff = false) { diff ->
        println("Live update: ${diff.value}")
    }.getOrThrow()
    // Later, stop the live query
    kill(queryUuid).getOrThrow()
}
```

### Running Custom Functions
Execute user-defined functions stored in SurrealDB:

```kotlin
withSurrealDB(config) {
    // Define a function (run once in setup)
    query<Unit>("DEFINE FUNCTION fn::greet($name) { RETURN 'Hello, ' + $name; }").getOrThrow()

    // Call the function
    val result = run<String>("fn::greet", null, listOf(JsonPrimitive("Alice"))).getOrThrow()
    println("Function result: $result") // Outputs: "Hello, Alice"
}
```

### Error Handling
Operations return a `Result<T>` type, allowing elegant error handling:

```kotlin
withSurrealDB(config) {
    val result = query<User>("SELECT * FROM nonexistent")
    when (result) {
        is Result.Success -> println("Users: ${result.data}")
        is Result.Error -> println("Query failed: ${result.exception.message}")
    }
}
```

## Configuration
The `SurrealDBClientConfig` class supports the following options:
- `url`: WebSocket endpoint (default: `"ws://localhost:8000/rpc"`).
- `namespace`: Namespace to use (optional).
- `database`: Database to use (optional).
- `credentials`: Authentication parameters (e.g., `SignInParams.Root("user", "pass")`).
- `verboseLogging`: Enable detailed WebSocket logging (default: `false`).

## Building from Source
1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/kotlin-surrealdb-driver.git
   cd kotlin-surrealdb-driver
   ```
2. Build with Gradle:
   ```bash
   ./gradlew build
   ```
3. Publish to your local Maven repository (optional):
   ```bash
   ./gradlew publishToMavenLocal
   ```

## Running Tests
The project includes a comprehensive test suite. Ensure SurrealDB is running locally:
```bash
docker run --rm -p 8000:8000 -e SURREAL_CAPS_ALLOW_EXPERIMENTAL=graphql surrealdb/surrealdb:latest start --user root --pass root --allow-all
```
Then run the tests:
```bash
./gradlew test
```

## TODO
- 3/26 tests are still failing

## Contributing
Contributions are welcome! Please:
1. Fork the repository.
2. Create a feature branch (`git checkout -b feature/your-feature`).
3. Commit your changes (`git commit -m "Add your feature"`).
4. Push to the branch (`git push origin feature/your-feature`).
5. Open a Pull Request.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgments
- Built with [Kotlin](https://kotlinlang.org/), [Ktor](https://ktor.io/), and [kotlinx.serialization](https://github.com/Kotlin/kotlinx.serialization).
- Inspired by the official SurrealDB clients and the Kotlin community.