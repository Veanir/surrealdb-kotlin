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
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test


@Serializable
data class User(val name: String, val age: Int)

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
                query<Unit>("DELETE user")
                println("User table cleared")
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
        // Act
        val result = signup(
            SignUpParams(
                NS = "test_namespace",
                DB = "test_database",
                AC = "allusers",
                username = "testuser",
                password = "testpass",
                additionalVars = mapOf("email" to JsonPrimitive("test@example.com"))
            )
        )

        // Assert
        result.shouldBeInstanceOf<Result.Success<String?>>()
        val token = result.data
        token.shouldNotBeNull()
        println("Signup token: $token")

        // Verify user creation
        val users = query<Map<String, JsonElement>>("SELECT username, email FROM user WHERE username = 'testuser'").getOrThrow()
        users.shouldHaveSize(1)
        val user = users[0]
        user["username"]?.jsonPrimitive?.content shouldBe "testuser"
        user["email"]?.jsonPrimitive?.content shouldBe "test@example.com"

        // Verify token usability
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
        users.any { it == User("Alice", 30) } shouldBe true
        users.any { it == User("Bob", 25) } shouldBe true
        return@withSurrealDB
    }

    @Test
    fun `query should return single result with first`() = withSurrealDB(config) {
        query<User>("CREATE user:charlie SET name = 'Charlie', age = 35")

        val user = query<User>("SELECT * FROM user:charlie").getOrThrow().first()
        user shouldBe User("Charlie", 35)
        return@withSurrealDB
    }

    @Test
    fun `query should handle multiple results`() = withSurrealDB(config) {
        query<Unit>("DELETE user WHERE age > 30")
        query<User>("CREATE user:multi1 SET name = 'Multi1', age = 40")
        query<User>("CREATE user:multi2 SET name = 'Multi2', age = 45")

        val users = query<User>("SELECT * FROM user WHERE age > 30").getOrThrow()
        users.shouldHaveSize(2)
        users.any { it == User("Multi1", 40) } shouldBe true
        users.any { it == User("Multi2", 45) } shouldBe true
        return@withSurrealDB
    }

    @Test
    fun `queryRaw should return raw JSON results`() = withSurrealDB(config) {
        query<User>("CREATE user:invalid SET name = 'Invalid', unexpected = 'data'")

        val result = queryRaw("SELECT * FROM user:invalid").getOrThrow()
        result.shouldHaveSize(1)
        val firstResult = result[0].jsonObject
        firstResult["name"]?.jsonPrimitive?.content shouldBe "Invalid"
        firstResult["unexpected"]?.jsonPrimitive?.content shouldBe "data"
        return@withSurrealDB
    }

    @Test
    fun `graphql should execute query`() = withSurrealDB(config) {
        create<User>(RecordId("user", "alice"), User("Alice", 18))
        val result = graphql<User>("query { user(age: 18) { name age } }", null)
        result.shouldBeInstanceOf<Result.Success<User>>()
        return@withSurrealDB
    }

    @Test
    fun `run should execute function`() = withSurrealDB(config) {
        query<Unit>("DEFINE FUNCTION fn::someFunction(\$param: String) { RETURN \$param; }")
        val result = run<String>("fn::someFunction", null, listOf(JsonPrimitive("hello")))
        result.shouldBeInstanceOf<Result.Success<String>>()
        val success = result as Result.Success
        success.data shouldBe "hello"
        return@withSurrealDB
    }

    @Test
    fun `live with callback should receive updates`() = withSurrealDB(config) {
        val updates = mutableListOf<Diff<User>>()
        val queryUuid = live("user", diff = false) { diff ->
            updates.add(diff)
        }.getOrThrow()
        delay(500)
        create(RecordId("user", "eve"), User("Eve", 28))
        delay(2000)
        kill(queryUuid).getOrThrow()

        updates.any { it.value == User("Eve", 28) } shouldBe true
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
                    create(RecordId("user", "frank"), User("Frank", 60)).getOrThrow()
                }
                flow.take(1).toList()
            }

            updates.any { it.value == User("Frank", 60) } shouldBe true
            println("Flow live updates received: $updates")
        }
    }

    @Test
    fun `select should retrieve records`() = withSurrealDB(config) {
        create(RecordId("user", "dave"), User("Dave", 50))
        val users = select<User>(RecordId("user", "dave")).getOrThrow()
        users.shouldHaveSize(1)
        users[0] shouldBe User("Dave", 50)
        return@withSurrealDB
    }

    @Test
    fun `create should add a record`() = withSurrealDB(config) {
        val user = create<User>(RecordId("user", "grace"), User("Grace", 22)).getOrThrow()
        user shouldBe User("Grace", 22)
        return@withSurrealDB
    }

    @Test
    fun `insert should add multiple records`() = withSurrealDB(config) {
        val users = listOf(User("Hank", 45), User("Ivy", 33))
        val result = insert("user", users).getOrThrow()
        result.shouldHaveSize(2)
        result.any { it == User("Hank", 45) } shouldBe true
        return@withSurrealDB
    }

    @Test
    fun `insertRelation should create a relation`() = withSurrealDB(config) {
        create(RecordId("user", "john"), User("John", 30))
        create(RecordId("user", "jane"), User("Jane", 28))
        val relation = insertRelation("friend", User("John", 30)).getOrThrow()
        relation.name shouldBe "John"
        return@withSurrealDB
    }

    @Test
    fun `update should modify a record`() = withSurrealDB(config) {
        create(RecordId("user", "kate"), User("Kate", 27))
        val updated = update(RecordId("user", "kate"), User("Kate", 28)).getOrThrow()
        updated shouldBe User("Kate", 28)
        return@withSurrealDB
    }

    @Test
    fun `upsert should create or update a record`() = withSurrealDB(config) {
        val upserted = upsert(RecordId("user", "leo"), User("Leo", 35)).getOrThrow()
        upserted shouldBe User("Leo", 35)

        val updated = upsert(RecordId("user", "leo"), User("Leo", 36)).getOrThrow()
        updated shouldBe User("Leo", 36)
        return@withSurrealDB
    }

    @Test
    fun `relate should link records`() = withSurrealDB(config) {
        create(RecordId("user", "mike"), User("Mike", 40))
        create(RecordId("user", "nina"), User("Nina", 38))
        val relation = relate<User>(
            RecordId("user", "mike"),
            "friend",
            RecordId("user", "nina"),
            User("Mike", 40)
        ).getOrThrow()
        relation.name shouldBe "Mike"
        return@withSurrealDB
    }

    @Test
    fun `merge should partially update a record`() = withSurrealDB(config) {
        create(RecordId("user", "olga"), User("Olga", 29))
        val merged = merge(RecordId("user", "olga"), User("Olga", 30)).getOrThrow()
        merged shouldBe User("Olga", 30)
        return@withSurrealDB
    }

    @Test
    fun `patch should apply patches to a record`() = withSurrealDB(config) {
        create(RecordId("user", "paul"), User("Paul", 25))
        val patches = listOf(Patch("replace", "age", JsonPrimitive(26)))
        val patched = patch<User>(RecordId("user", "paul"), patches, diff = false).getOrThrow()
        patched[0].age shouldBe 26
        return@withSurrealDB
    }

    @Test
    fun `delete should remove a record`() = withSurrealDB(config) {
        create(RecordId("user", "quinn"), User("Quinn", 31))
        val deleted = delete<User>(RecordId("user", "quinn")).getOrThrow()
        deleted.shouldHaveSize(1)
        deleted[0] shouldBe User("Quinn", 31)

        val selectResult = select<User>(RecordId("user", "quinn")).getOrThrow()
        selectResult.shouldHaveSize(0)
        return@withSurrealDB
    }
}