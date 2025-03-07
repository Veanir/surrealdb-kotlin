package pl.steclab.surrealdb

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.*
import io.ktor.client.*
import io.ktor.client.plugins.websocket.*
import io.ktor.websocket.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*
import kotlin.reflect.KClass

@Serializable
data class User(val name: String, val age: Int)

class SurrealDBClientTest : FunSpec({
    // Mock setup for unit tests
    val mockHttpClient = mockk<HttpClient>(relaxed = true)
    val mockSession = mockk<DefaultClientWebSocketSession>(relaxed = true)
    val mockSendChannel = mockk<Channel<String>>(relaxed = true)

    // Helper to create a testable client with mocks
    fun createMockClient(): SurrealDBClient {
        val client = spyk(SurrealDBClient("ws://localhost:8000/rpc"))

        // Mock the HttpClient behavior
        coEvery { mockHttpClient.webSocket(any(), any(), any()) } coAnswers {
            thirdArg<suspend DefaultClientWebSocketSession.() -> Unit>().invoke(mockSession)
        }
        coEvery { mockSendChannel.send(any()) } just Runs
        coEvery { mockSendChannel.close() } returns true

        // Inject mocks via reflection or modify class if needed (avoiding private field access)
        return client
    }

    // Integration test setup
    val realClient = SurrealDBClient("ws://localhost:8000/rpc")

    beforeSpec {
        runBlocking { realClient.connect() }
    }

    afterSpec {
        runBlocking { realClient.disconnect() }
    }

    // Unit Tests
    test("connect should establish WebSocket connection") {
        val client = createMockClient()

        runBlocking { client.connect() }
        coVerify { mockHttpClient.webSocket("ws://localhost:8000/rpc", any(), any()) }
    }

    test("query should return list of results") {
        val client = createMockClient()
        val response = buildJsonArray {
            add(buildJsonObject {
                put("result", buildJsonArray {
                    add(buildJsonObject { put("name", "Alice"); put("age", 30) })
                    add(buildJsonObject { put("name", "Bob"); put("age", 25) })
                })
            })
        }
        coEvery { client.sendRpcRequest("query", any()) } returns response

        val result = runBlocking { client.query("SELECT * FROM user", null, User::class, lenient = false) }
        result.shouldBeInstanceOf<Result.Success<List<User>>>()
        val users: List<User> = (result as Result.Success<List<User>>).data
        users.shouldHaveSize(2)
        users[0] shouldBe User("Alice", 30)
        users[1] shouldBe User("Bob", 25)
    }

    test("querySingle should return single result") {
        val client = createMockClient()
        val response = buildJsonArray {
            add(buildJsonObject {
                put("result", buildJsonArray {
                    add(buildJsonObject { put("name", "Alice"); put("age", 30) })
                })
            })
        }
        coEvery { client.sendRpcRequest("query", any()) } returns response

        val result = runBlocking { client.querySingle("SELECT * FROM user:1", null, User::class, lenient = false) }
        result.shouldBeInstanceOf<Result.Success<Any>>()
        val user = (result as Result.Success<Any>).data
        user shouldBe User("Alice", 30)
    }

    test("querySingle should fail with multiple results") {
        val client = createMockClient()
        val response = buildJsonArray {
            add(buildJsonObject {
                put("result", buildJsonArray {
                    add(buildJsonObject { put("name", "Alice"); put("age", 30) })
                    add(buildJsonObject { put("name", "Bob"); put("age", 25) })
                })
            })
        }
        coEvery { client.sendRpcRequest("query", any()) } returns response

        val result = runBlocking { client.querySingle("SELECT * FROM user", null, User::class, lenient = false) }
        result.shouldBeInstanceOf<Result.Error<*>>()
        (result as Result.Error).exception.message shouldBe "Expected one result, got 2"
    }

    test("querySingle with lenient mode should return JsonElement on failure") {
        val client = createMockClient()
        val rawJson = buildJsonArray {
            add(buildJsonObject {
                put("result", buildJsonObject { put("name", "Alice"); put("unexpected", "data") })
            })
        }
        coEvery { client.sendRpcRequest("query", any()) } returns rawJson

        val result = runBlocking { client.querySingle("SELECT * FROM user:1", null, User::class, lenient = true) }
        result.shouldBeInstanceOf<Result.Success<Any>>()
        val data = (result as Result.Success<Any>).data
        data.shouldBeInstanceOf<JsonElement>()
        data.toString() shouldBe rawJson.toString()
    }

    test("create should return created record") {
        val client = createMockClient()
        val response = buildJsonArray {
            add(buildJsonObject { put("name", "Alice"); put("age", 30) })
        }
        coEvery { client.sendRpcRequest("create", any()) } returns response

        val result = runBlocking { client.create(RecordId("user", "1"), User("Alice", 30), User::class) }
        result.shouldBeInstanceOf<Result.Success<User>>()
        (result as Result.Success<User>).data shouldBe User("Alice", 30)
    }

    test("live should register callback and return queryUuid") {
        val client = createMockClient()
        val queryUuid = "live-uuid-123"
        coEvery { client.sendRpcRequest("live", any()) } returns JsonPrimitive(queryUuid)

        var capturedDiff: Diff<User>? = null
        val result = runBlocking {
            client.live("user", diff = false, User::class) { diff ->
                capturedDiff = diff
            }
        }
        result.shouldBeInstanceOf<Result.Success<String>>()
        (result as Result.Success<String>).data shouldBe queryUuid
    }

    // Integration Tests
    test("integration: query should fetch real data").config(enabled = true) {
        runBlocking {
            realClient.querySingle(
                "CREATE user:alice SET name = 'Alice', age = 30",
                null,
                User::class,
                lenient = false
            ) // Setup data

            val result = realClient.query("SELECT * FROM user", null, User::class, lenient = false)
            result.shouldBeInstanceOf<Result.Success<List<User>>>()
            val users: List<User> = (result as Result.Success<List<User>>).data
            users.any { it.name == "Alice" && it.age == 30 } shouldBe true
        }
    }

    test("integration: querySingle should fetch single record").config(enabled = true) {
        runBlocking {
            realClient.querySingle(
                "CREATE user:bob SET name = 'Bob', age = 25",
                null,
                User::class,
                lenient = false
            ) // Setup data

            val result = realClient.querySingle("SELECT * FROM user:bob", null, User::class, lenient = false)
            result.shouldBeInstanceOf<Result.Success<Any>>()
            val user = (result as Result.Success<Any>).data
            user shouldBe User("Bob", 25)
        }
    }

    test("integration: live query should receive updates").config(enabled = true) {
        runBlocking {
            val updates = mutableListOf<Diff<User>>()
            val result = realClient.live("user", diff = false, User::class) { diff ->
                updates.add(diff)
            }
            result.shouldBeInstanceOf<Result.Success<String>>()
            val queryUuid = (result as Result.Success<String>).data

            delay(100) // Wait for subscription
            realClient.querySingle("CREATE user:charlie SET name = 'Charlie', age = 35", null, User::class, lenient = false)
            delay(500) // Wait for live update

            updates.any { it.value?.name == "Charlie" && it.value?.age == 35 } shouldBe true
            realClient.kill(queryUuid) // Cleanup
        }
    }
})