package pl.steclab.surrealdb

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.datatest.withData
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import pl.steclab.surrealdb.client.SurrealDBClientConfig
import pl.steclab.surrealdb.model.Diff
import pl.steclab.surrealdb.model.RecordId
import pl.steclab.surrealdb.model.SignInParams
import pl.steclab.surrealdb.model.SignUpParams
import pl.steclab.surrealdb.result.getOrThrow
import pl.steclab.surrealdb.result.Result
import pl.steclab.surrealdb.utils.SurrealDBOperations
import pl.steclab.surrealdb.utils.withSurrealDB
import java.util.*

@Serializable
data class User(
    val id: RecordId? = null,
    val name: String,
    val age: Int,
    val friend: RecordId? = null
)

@Serializable
data class Friendship(
    val id: RecordId? = null,
    val `in`: RecordId,
    val out: RecordId,
    val since: Long
)

class SurrealDBClientTest : FunSpec() {

    companion object {
        private val config = SurrealDBClientConfig().apply {
            url = "ws://localhost:8000/rpc"
            namespace = "test_namespace"
            database = "test_database"
            credentials = SignInParams.Root("root", "root")
            verboseLogging = true
        }

        private val json = Json { ignoreUnknownKeys = true }

        @BeforeAll
        @JvmStatic
        fun setup() {
            try {
                withSurrealDB(config) {
                    query<Unit>("DELETE user").getOrThrow()
                    query<Unit>("DELETE friendship").getOrThrow()
                    query<Unit>(
                        """
                        DEFINE ACCESS allusers ON DATABASE
                        TYPE JWT
                        ALGORITHM HS512 KEY 'your-secret-key-here'
                        """
                    ).getOrThrow()
                    query<Unit>(
                        """
                        DEFINE ACCESS record_users ON DATABASE
                        TYPE RECORD
                        SIGNUP (CREATE user SET username = ${'$'}username, password = crypto::argon2::generate(${'$'}password), email = ${'$'}email)
                        SIGNIN (SELECT * FROM user WHERE username = ${'$'}username AND crypto::argon2::compare(password, ${'$'}password))
                        WITH JWT ALGORITHM HS512 KEY 'your-secret-key-here'
                        """
                    ).getOrThrow()
                    println("Database initialized with access methods")
                }
            } catch (e: Exception) {
                throw RuntimeException("Failed to set up SurrealDB for tests", e)
            }
        }

        @AfterAll
        @JvmStatic
        fun teardown() {
            println("Tests completed, resources cleaned up")
        }
    }

    @BeforeEach
    fun resetDatabase() {
        withSurrealDB(config) {
            query<Unit>("DELETE user").getOrThrow()
            query<Unit>("DELETE friendship").getOrThrow()
            return@withSurrealDB
        }
    }

    private fun uniqueId(prefix: String) = "$prefix${UUID.randomUUID().toString().substring(0, 8)}"

    // Add createUser helper as an extension function
    private suspend fun SurrealDBOperations.createUser(id: String, name: String, age: Int): User {
        return create(RecordId("user", id), User(null, name, age)).getOrThrow()
    }

    init {
        test("connect should establish WebSocket connection") {
            withSurrealDB(config) {
                val result = version()
                result.shouldBeInstanceOf<Result.Success<JsonElement>>()
                val version = (result as Result.Success).data.jsonPrimitive.content
                version.shouldNotBeNull()
                println("Connected to SurrealDB version: $version")
                return@withSurrealDB
            }
        }

        test("connect should fail with invalid URL") {
            val badConfig = config.copy(url = "ws://invalid:9999/rpc")
            shouldThrow<Exception> {
                withSurrealDB(badConfig) {
                    version()
                }
            }
        }

        test("use should set namespace and database") {
            withSurrealDB(config) {
                val result = use("test_namespace", "test_database")
                result.shouldBeInstanceOf<Result.Success<JsonElement>>()
                return@withSurrealDB
            }
        }

        test("info should return session info") {
            withSurrealDB(config) {
                val result = info()
                result.shouldBeInstanceOf<Result.Success<JsonElement>>()
                return@withSurrealDB
            }
        }

        test("signup should create user and return valid token") {
            withSurrealDB(config) {
                val username = uniqueId("testuser")
                val result = signup(
                    SignUpParams(
                        NS = "test_namespace",
                        DB = "test_database",
                        AC = "record_users",
                        username = username,
                        password = "testpass",
                        additionalVars = mapOf("email" to JsonPrimitive("test@example.com"))
                    )
                )
                result.shouldBeInstanceOf<Result.Success<String?>>()
                val token = result.data.shouldNotBeNull()

                val payloadJson = json.parseToJsonElement(//Unresolved reference 'json'.
                    String(java.util.Base64.getUrlDecoder().decode(token.split(".")[1]))
                ).jsonObject
                val userId = payloadJson["ID"]?.jsonPrimitive?.content ?: throw AssertionError("User ID not found")

                @Serializable
                data class UserCredentials(val username: String, val email: String)

                val users = query<UserCredentials>("SELECT username, email FROM $userId").getOrThrow()
                users.shouldHaveSize(1)
                users[0] shouldBe UserCredentials(username, "test@example.com")

                authenticate(token).shouldBeInstanceOf<Result.Success<Unit>>()
                return@withSurrealDB
            }
        }

        test("signin should authenticate user") {
            withSurrealDB(config) {
                val result = signin(SignInParams.Root("root", "root"))
                result.shouldBeInstanceOf<Result.Success<String?>>()
                result.data.shouldNotBeNull()
                return@withSurrealDB
            }
        }

        test("signin should fail with invalid credentials") {
            withSurrealDB(config) {
                val result = signin(SignInParams.Root("root", "wrongpass"))
                result.shouldBeInstanceOf<Result.Failure>()
                return@withSurrealDB
            }
        }

        test("authenticate should validate token") {
            withSurrealDB(config) {
                val token = signin(SignInParams.Root("root", "root")).getOrThrow()
                token?.let {
                    val result = authenticate(it)
                    result.shouldBeInstanceOf<Result.Success<Unit>>()
                }
                return@withSurrealDB
            }
        }

        test("invalidate should clear session") {
            withSurrealDB(config) {
                val result = invalidate()
                result.shouldBeInstanceOf<Result.Success<Unit>>()
                return@withSurrealDB
            }
        }

        test("let and unset should manage variables") {
            withSurrealDB(config) {
                let("testVar", JsonPrimitive("value")).shouldBeInstanceOf<Result.Success<Unit>>()
                unset("testVar").shouldBeInstanceOf<Result.Success<Unit>>()
                return@withSurrealDB
            }
        }

        test("query should return list of results") {
            withSurrealDB(config) {
                createUser(uniqueId("alice"), "Alice", 30)
                createUser(uniqueId("bob"), "Bob", 25)
                val users = query<User>("SELECT * FROM user").getOrThrow()
                users.shouldHaveSize(2)
                users.map { it.name to it.age } shouldContainExactly listOf("Alice" to 30, "Bob" to 25)
                return@withSurrealDB
            }
        }

        test("query should return single result with first") {
            withSurrealDB(config) {
                val id = uniqueId("charlie")
                createUser(id, "Charlie", 35)
                val user = query<User>("SELECT * FROM user:$id").getOrThrow().first()
                user shouldBe User(RecordId("user", id), "Charlie", 35)
                return@withSurrealDB
            }
        }

        test("queryRaw should return raw JSON results") {
            withSurrealDB(config) {
                val id = uniqueId("invalid")
                queryRaw("CREATE user:$id SET name = 'Invalid', unexpected = 'data'").getOrThrow()
                val result = queryRaw("SELECT * FROM user:$id").getOrThrow()
                result.shouldHaveSize(1)
                val json = result[0].jsonObject
                json["name"]?.jsonPrimitive?.content shouldBe "Invalid"
                json["unexpected"]?.jsonPrimitive?.content shouldBe "data"
                return@withSurrealDB
            }
        }

        test("graphql should execute query") {
            withSurrealDB(config) {
                createUser(uniqueId("alice"), "Alice", 18)
                val result = graphql<User>("query { user(age: 18) { name age } }", null).getOrThrow()
                result shouldBe User(null, "Alice", 18)
                return@withSurrealDB
            }
        }

        test("run should execute function") {
            withSurrealDB(config) {
                query<Unit>("REMOVE FUNCTION fn::someFunction").onSuccess { println("Cleaned up fn::someFunction") }
                query<Unit>("DEFINE FUNCTION fn::someFunction(\$param: string) { RETURN \$param; }").getOrThrow()
                val result = run<String>("fn::someFunction", null, listOf(JsonPrimitive("hello"))).getOrThrow()
                result shouldBe "hello"
                return@withSurrealDB
            }
        }

        context("live query tests") {
            test("live with callback should handle CREATE, UPDATE, DELETE without diff") {
                withSurrealDB(config) {
                    val updates = mutableListOf<Diff<User>>()
                    val queryUuid = live("user", diff = false) { diff ->
                        synchronized(updates) { updates.add(diff) }
                    }.getOrThrow()

                    delay(100) // Wait for subscription
                    val userId = uniqueId("john")
                    val created = createUser(userId, "John", 30)
                    delay(100)
                    update(RecordId("user", userId), User(null, "John Updated", 31)).getOrThrow()
                    delay(100)
                    delete<User>(RecordId("user", userId)).getOrThrow()
                    delay(500) // Ensure all notifications are received

                    kill(queryUuid).getOrThrow()

                    synchronized(updates) {
                        updates.shouldHaveSize(3)
                        val createDiff = updates[0] // Order-based: first event is CREATE
                        val updateDiff = updates[1] // Second event is UPDATE
                        val deleteDiff = updates[2] // Third event is DELETE

                        createDiff.operation shouldBe "create"
                        createDiff.path shouldBe null
                        createDiff.value shouldBe created

                        updateDiff.operation shouldBe "update"
                        updateDiff.path shouldBe null
                        updateDiff.value?.id shouldBe RecordId("user", userId)
                        updateDiff.value?.name shouldBe "John Updated"
                        updateDiff.value?.age shouldBe 31

                        deleteDiff.operation shouldBe "delete"
                        deleteDiff.path shouldBe null
                        deleteDiff.value?.id shouldBe RecordId("user", userId)
                        deleteDiff.value?.name shouldBe "John Updated"
                        deleteDiff.value?.age shouldBe 31
                    }
                    println("Callback updates (no diff): $updates")
                    return@withSurrealDB
                }
            }

            test("live with Flow should handle CREATE, UPDATE, DELETE without diff") {
                runBlocking {
                    withSurrealDB(config) {
                        val flow = live<User>("user", diff = false).getOrThrow()
                        val updates = mutableListOf<Diff<User>>()

                        launch {
                            delay(100)
                            val userId = uniqueId("mary")
                            createUser(userId, "Mary", 25)
                            delay(100)
                            update(RecordId("user", userId), User(null, "Mary Updated", 26)).getOrThrow()
                            delay(100)
                            delete<User>(RecordId("user", userId)).getOrThrow()
                        }

                        withTimeout(1000) {
                            flow.take(3).toList(updates)
                        }

                        updates.shouldHaveSize(3)
                        val createDiff = updates[0]
                        val updateDiff = updates[1]
                        val deleteDiff = updates[2]

                        createDiff.operation shouldBe "create"
                        createDiff.value?.name shouldBe "Mary"
                        createDiff.value?.age shouldBe 25

                        updateDiff.operation shouldBe "update"
                        updateDiff.value?.name shouldBe "Mary Updated"
                        updateDiff.value?.age shouldBe 26

                        deleteDiff.operation shouldBe "delete"
                        deleteDiff.value?.name shouldBe "Mary Updated"
                        deleteDiff.value?.age shouldBe 26

                        println("Flow updates (no diff): $updates")
                        return@withSurrealDB
                    }
                }
            }

            test("live with callback should handle diff mode") {
                withSurrealDB(config) {
                    val updates = mutableListOf<Diff<User>>()
                    val queryUuid = live("user", diff = true) { diff ->
                        synchronized(updates) { updates.add(diff) }
                    }.getOrThrow()

                    delay(100)
                    val userId = uniqueId("pat")
                    createUser(userId, "Pat", 40)
                    delay(100)
                    update(RecordId("user", userId), User(null, "Pat", 41)).getOrThrow()
                    delay(100)
                    delete<User>(RecordId("user", userId)).getOrThrow()
                    delay(500)

                    kill(queryUuid).getOrThrow()

                    synchronized(updates) {
                        updates.forEach { println("Diff update: $it") }
                        updates.shouldHaveSize(3)
                        updates[0].operation shouldBe "replace" // CREATE sends full object replacement
                        updates[0].path shouldBe "/"
                        updates[0].value?.name shouldBe "Pat"
                        updates[0].value?.age shouldBe 40

                        updates[1].operation shouldBe "replace"
                        updates[1].path shouldBe "/age"
                        updates[1].value?.age shouldBe 41

                        updates[2].operation shouldBe "remove"
                        updates[2].path shouldBe ""
                        updates[2].value?.name shouldBe "Pat"
                        updates[2].value?.age shouldBe 41
                    }
                    println("Callback updates (diff): $updates")
                    return@withSurrealDB
                }
            }

            test("live with Flow should handle diff mode") {
                runBlocking {
                    withSurrealDB(config) {
                        val flow = live<User>("user", diff = true).getOrThrow()
                        val updates = mutableListOf<Diff<User>>()

                        launch {
                            delay(100)
                            val userId = uniqueId("sam")
                            createUser(userId, "Sam", 50)
                            delay(100)
                            update(RecordId("user", userId), User(null, "Sam", 51)).getOrThrow()
                            delay(100)
                            delete<User>(RecordId("user", userId)).getOrThrow()
                        }

                        withTimeout(1000) {
                            flow.take(3).toList(updates) // Adjust based on actual patch count
                        }

                        updates.forEach { println("Diff Flow update: $it") }
                        updates.any { it.operation == "add" && it.path == "/name" && it.value?.name == "Sam" } shouldBe true
                        updates.any { it.operation == "replace" && it.path == "/age" && it.value?.age == 51 } shouldBe true
                        updates.any { it.operation == "remove" && it.path == "" } shouldBe true

                        println("Flow updates (diff): $updates")
                        return@withSurrealDB
                    }
                }
            }
        }

        test("create and select should handle RecordId correctly") {
            withSurrealDB(config) {
                val userId = RecordId("user", uniqueId("alice"))
                val user = create(userId, User(null, "Alice", 30)).getOrThrow()
                user shouldBe User(userId, "Alice", 30)

                val selected = select<User>(userId).getOrThrow()
                selected shouldContainExactly listOf(user)
                return@withSurrealDB
            }
        }

        test("create with reference should store RecordId") {
            withSurrealDB(config) {
                val friendId = RecordId("user", uniqueId("bob"))
                create(friendId, User(null, "Bob", 25)).getOrThrow()

                val userId = RecordId("user", uniqueId("alice"))
                val user = create(userId, User(null, "Alice", 30, friendId)).getOrThrow()
                user.friend shouldBe friendId

                val selected = select<User>(userId).getOrThrow()
                selected[0].friend shouldBe friendId
                return@withSurrealDB
            }
        }

        test("relate should create relationship with RecordId references") {
            withSurrealDB(config) {
                val user1Id = RecordId("user", uniqueId("mike"))
                val user2Id = RecordId("user", uniqueId("nina"))
                create(user1Id, User(null, "Mike", 40)).getOrThrow()
                create(user2Id, User(null, "Nina", 38)).getOrThrow()

                val friendship = relate<Friendship>(
                    user1Id, "friendship", user2Id,
                    Friendship(null, user1Id, user2Id, System.currentTimeMillis())
                ).getOrThrow()

                friendship.`in` shouldBe user1Id
                friendship.out shouldBe user2Id

                val selected = select<Friendship>(friendship.id!!).getOrThrow()
                selected[0] shouldBe friendship
                return@withSurrealDB
            }
        }

        test("insert should add multiple records") {
            withSurrealDB(config) {
                val users = listOf(User(null, "Hank", 45), User(null, "Ivy", 33))
                val result = insert("user", users).getOrThrow()
                result.shouldHaveSize(2)
                result.map { it.name to it.age } shouldContainExactly listOf("Hank" to 45, "Ivy" to 33)
                return@withSurrealDB
            }
        }

        test("update should modify a record") {
            withSurrealDB(config) {
                val userId = RecordId("user", uniqueId("kate"))
                create(userId, User(null, "Kate", 27)).getOrThrow()
                val updated = update(userId, User(userId, "Kate", 28)).getOrThrow()
                updated shouldBe User(userId, "Kate", 28)
                return@withSurrealDB
            }
        }

        test("delete should remove a record") {
            withSurrealDB(config) {
                val userId = RecordId("user", uniqueId("quinn"))
                create(userId, User(null, "Quinn", 31)).getOrThrow()
                val deleted = delete<User>(userId).getOrThrow()
                deleted.shouldHaveSize(1)
                deleted[0] shouldBe User(userId, "Quinn", 31)

                val selectResult = select<User>(userId).getOrThrow()
                selectResult.shouldHaveSize(0)
                return@withSurrealDB
            }
        }

        test("select should return empty list for non-existent record") {
            withSurrealDB(config) {
                val result = select<User>(RecordId("user", "nonexistent")).getOrThrow()
                result.shouldHaveSize(0)
                return@withSurrealDB
            }
        }

        context("data-driven tests") {
            withData(
                nameFn = { "create should handle age $it" },
                listOf(18, 30, 99)
            ) { age ->
                withSurrealDB(config) {
                    val userId = RecordId("user", uniqueId("test"))
                    val user = create(userId, User(null, "Test", age)).getOrThrow()
                    user.age shouldBe age
                    return@withSurrealDB
                }
            }
        }
    }
}