package pl.steclab.surrealdb

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
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

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

class SurrealDBClientTest {

    companion object {
        private val config = SurrealDBClientConfig().apply {
            url = "ws://localhost:8000/rpc"
            namespace = "test_namespace"
            database = "test_database"
            credentials = SignInParams.Root("root", "root")
            verboseLogging = true
        }

        @BeforeAll
        @JvmStatic
        fun setup() {
            withSurrealDB(config) {
                query<Unit>("DELETE user").getOrThrow()
                query<Unit>("DELETE friendship").getOrThrow()
                // Define a JWT-based access method for signup
                query<Unit>(
                    """
            DEFINE ACCESS allusers ON DATABASE
            TYPE JWT
            ALGORITHM HS512 KEY 'your-secret-key-here'
            """
                ).getOrThrow()
                // Define a record-based access method with signup/signin if needed
                query<Unit>(
                    """
            DEFINE ACCESS record_users ON DATABASE
            TYPE RECORD
            SIGNUP (CREATE user SET username = ${'$'}username, password = crypto::argon2::generate(${'$'}password), email = ${'$'}email)
            SIGNIN (SELECT * FROM user WHERE username = ${'$'}username AND crypto::argon2::compare(password, ${'$'}password))
            WITH JWT ALGORITHM HS512 KEY 'your-secret-key-here'
            """
                ).getOrThrow()
                println("User and friendship tables cleared, access methods 'allusers' and 'record_users' defined")
            }
        }

        @AfterAll
        @JvmStatic
        fun teardown() {
            println("Tests completed, resources cleaned up by withSurrealDB")
        }
    }

    @Test
    fun `connect should establish WebSocket connection`() = withSurrealDB(config) {
        val result = version()
        result.shouldBeInstanceOf<Result.Success<JsonElement>>()
        println("Version: ${(result as Result.Success).data}")
        return@withSurrealDB
    }

    @Test
    fun `use should set namespace and database`() = withSurrealDB(config) {
        val result = use("test_namespace", "test_database")
        result.shouldBeInstanceOf<Result.Success<JsonElement>>()
        return@withSurrealDB
    }

    @Test
    fun `info should return session info`() = withSurrealDB(config) {
        val result = info()
        result.shouldBeInstanceOf<Result.Success<JsonElement>>()
        return@withSurrealDB
    }

    @Test
    fun `signup should create a new user and return a valid token`() = withSurrealDB(config) {
        query<Unit>("DELETE user WHERE username = 'testuser'").onSuccess { println("Cleaned up existing testuser") }

        val result = signup(
            SignUpParams(
                NS = "test_namespace",
                DB = "test_database",
                AC = "record_users",
                username = "testuser",
                password = "testpass",
                additionalVars = mapOf("email" to JsonPrimitive("test@example.com"))
            )
        )
        result.shouldBeInstanceOf<Result.Success<String?>>()
        val token = result.data
        token.shouldNotBeNull()
        println("Signup token: $token")

        // Parse the JWT token to extract the user ID
        val tokenParts = token.split(".")
        val payload = tokenParts[1] // Base64-encoded payload
        val decodedPayload = String(java.util.Base64.getUrlDecoder().decode(payload))
        val payloadJson = json.parseToJsonElement(decodedPayload).jsonObject
        val userId = payloadJson["ID"]?.jsonPrimitive?.content ?: throw AssertionError("User ID not found in token")
        println("User ID from token: $userId")

        // Define a serializable data class for the query result
        @Serializable
        data class UserCredentials(val username: String, val email: String)

        // Query by the user ID from the token
        val users = query<UserCredentials>("SELECT username, email FROM $userId").getOrThrow()
        users.shouldHaveSize(1)
        val user = users[0]
        user.username shouldBe "testuser"
        user.email shouldBe "test@example.com"

        val authResult = authenticate(token)
        authResult.shouldBeInstanceOf<Result.Success<Unit>>()
        return@withSurrealDB
    }

    @Test
    fun `signin should authenticate user`() = withSurrealDB(config) {
        val result = signin(SignInParams.Root("root", "root"))
        result.shouldBeInstanceOf<Result.Success<String?>>()
        return@withSurrealDB
    }

    @Test
    fun `authenticate should validate token`() = withSurrealDB(config) {
        val token = signin(SignInParams.Root("root", "root")).getOrThrow()
        token?.let {
            val result = authenticate(it)
            result.shouldBeInstanceOf<Result.Success<Unit>>()
        }
        return@withSurrealDB
    }

    @Test
    fun `invalidate should clear session`() = withSurrealDB(config) {
        val result = invalidate()
        result.shouldBeInstanceOf<Result.Success<Unit>>()
        return@withSurrealDB
    }

    @Test
    fun `let and unset should manage variables`() = withSurrealDB(config) {
        val setResult = let("testVar", JsonPrimitive("value"))
        setResult.shouldBeInstanceOf<Result.Success<Unit>>()

        val unsetResult = unset("testVar")
        unsetResult.shouldBeInstanceOf<Result.Success<Unit>>()
        return@withSurrealDB
    }

    @Test
    fun `query should return list of results`() = withSurrealDB(config) {
        query<Unit>("DELETE user").getOrThrow()
        query<User>("CREATE user:alice SET name = 'Alice', age = 30").getOrThrow()
        query<User>("CREATE user:bob SET name = 'Bob', age = 25").getOrThrow()
        val users = query<User>("SELECT * FROM user").getOrThrow()
        users.shouldHaveSize(2)
        users.any { it == User(RecordId("user", "alice"), "Alice", 30) } shouldBe true
        users.any { it == User(RecordId("user", "bob"), "Bob", 25) } shouldBe true
        return@withSurrealDB
    }

    @Test
    fun `query should return single result with first`() = withSurrealDB(config) {
        query<User>("CREATE user:charlie SET name = 'Charlie', age = 35").getOrThrow()
        val user = query<User>("SELECT * FROM user:charlie").getOrThrow().first()
        user shouldBe User(RecordId("user", "charlie"), "Charlie", 35)
        return@withSurrealDB
    }

    @Test
    fun `queryRaw should return raw JSON results`() = withSurrealDB(config) {
        delete<User>(RecordId("user", "invalid")).onSuccess { println("Cleaned up existing user:invalid") }
        queryRaw("CREATE user:invalid SET name = 'Invalid', unexpected = 'data'").getOrThrow()

        val result = queryRaw("SELECT * FROM user:invalid").getOrThrow()
        result.shouldHaveSize(1)
        val firstResult = result[0].jsonObject
        firstResult["name"]?.jsonPrimitive?.content shouldBe "Invalid"
        firstResult["unexpected"]?.jsonPrimitive?.content shouldBe "data"
        return@withSurrealDB
    }

    @Test
    fun `graphql should execute query`() = withSurrealDB(config) {
        create(RecordId("user", "alice"), User(null, "Alice", 18)).getOrThrow()
        val result = graphql<User>("query { user(age: 18) { name age } }", null).getOrThrow()
        result.name shouldBe "Alice"
        result.age shouldBe 18
        return@withSurrealDB
    }

    @Test
    fun `run should execute function`() = withSurrealDB(config) {
        query<Unit>("REMOVE FUNCTION fn::someFunction").onSuccess { println("Cleaned up existing function fn::someFunction") }
        query<Unit>("DEFINE FUNCTION fn::someFunction(\$param: string) { RETURN \$param; }").getOrThrow()
        val result = run<String>("fn::someFunction", null, listOf(JsonPrimitive("hello"))).getOrThrow()
        result shouldBe "hello"
        return@withSurrealDB
    }

    @Test
    fun `live with callback should receive updates`() = withSurrealDB(config) {
        val updates = mutableListOf<Diff<User>>()
        val queryUuid = live("user", diff = false) { diff ->
            updates.add(diff)
        }.getOrThrow()
        delay(500)
        create(RecordId("user", "eve"), User(null, "Eve", 28)).getOrThrow()
        delay(2000)
        kill(queryUuid).getOrThrow()
        updates.any { it.value?.name == "Eve" && it.value?.age == 28 } shouldBe true
        println("Callback live updates received: $updates")
        return@withSurrealDB
    }

    @Test
    fun `live with Flow should emit updates`() = runBlocking {
        withSurrealDB(config) {
            val updates = withTimeout(5000) {
                val flow = live<User>("user").getOrThrow()
                delay(500)
                launch {
                    create(RecordId("user", "frank"), User(null, "Frank", 60)).getOrThrow()
                }
                flow.take(1).toList()
            }
            updates.any { it.value?.name == "Frank" && it.value?.age == 60 } shouldBe true
            println("Flow live updates received: $updates")
            return@withSurrealDB
        }
    }

    @Test
    fun `create and select should handle RecordId correctly`() = withSurrealDB(config) {
        val userId = RecordId("user", "alice")
        delete<User>(userId).onSuccess { println("Cleaned up existing user:alice") }

        val user = create(userId, User(null, "Alice", 30)).getOrThrow()
        user.id shouldBe userId
        user.name shouldBe "Alice"
        user.age shouldBe 30

        val selected = select<User>(userId).getOrThrow()
        selected.shouldHaveSize(1)
        selected[0] shouldBe user
        return@withSurrealDB
    }

    @Test
    fun `create with reference should store RecordId`() = withSurrealDB(config) {
        val friendId = RecordId("user", "bob")
        create(friendId, User(null, "Bob", 25)).getOrThrow()

        val userId = RecordId("user", "alice")
        val user = create(userId, User(null, "Alice", 30, friendId)).getOrThrow()
        user.friend shouldBe friendId

        val selected = select<User>(userId).getOrThrow()
        selected[0].friend shouldBe friendId
        return@withSurrealDB
    }

    @Test
    fun `relate should create a relationship with RecordId references`() = withSurrealDB(config) {
        val user1Id = RecordId("user", "mike")
        val user2Id = RecordId("user", "nina")
        create(user1Id, User(null, "Mike", 40)).getOrThrow()
        create(user2Id, User(null, "Nina", 38)).getOrThrow()

        val friendship = relate<Friendship>(
            user1Id,
            "friendship",
            user2Id,
            Friendship(null, user1Id, user2Id, System.currentTimeMillis())
        ).getOrThrow()

        friendship.`in` shouldBe user1Id
        friendship.out shouldBe user2Id

        val selected = select<Friendship>(friendship.id!!).getOrThrow()
        selected[0].`in` shouldBe user1Id
        selected[0].out shouldBe user2Id
        return@withSurrealDB
    }

    @Test
    fun `insert should add multiple records`() = withSurrealDB(config) {
        val users = listOf(User(null, "Hank", 45), User(null, "Ivy", 33))
        val result = insert("user", users).getOrThrow()
        result.shouldHaveSize(2)
        result.any { it.name == "Hank" && it.age == 45 } shouldBe true
        result.any { it.name == "Ivy" && it.age == 33 } shouldBe true
        return@withSurrealDB
    }

    @Test
    fun `update should modify a record`() = withSurrealDB(config) {
        val userId = RecordId("user", "kate")
        create(userId, User(null, "Kate", 27)).getOrThrow()
        val updated = update(userId, User(userId, "Kate", 28)).getOrThrow()
        updated shouldBe User(userId, "Kate", 28)
        return@withSurrealDB
    }

    @Test
    fun `delete should remove a record`() = withSurrealDB(config) {
        val userId = RecordId("user", "quinn")
        create(userId, User(null, "Quinn", 31)).getOrThrow()
        val deleted = delete<User>(userId).getOrThrow()
        deleted.shouldHaveSize(1)
        deleted[0] shouldBe User(userId, "Quinn", 31)

        val selectResult = select<User>(userId).getOrThrow()
        selectResult.shouldHaveSize(0)
        return@withSurrealDB
    }
}