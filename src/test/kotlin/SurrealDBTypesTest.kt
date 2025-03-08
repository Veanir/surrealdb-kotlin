package pl.steclab.surrealdb

import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonPrimitive
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.OffsetDateTime
import java.util.*

class SurrealDBTypesTest {
    companion object {
        private val config = SurrealDBClientConfig(
            url = "ws://localhost:8000/rpc",
            namespace = "test",
            database = "test",
            credentials = SignInParams.Root("root", "root"),
            verboseLogging = true
        )

        fun uniqueId(prefix: String) = "$prefix${UUID.randomUUID().toString().substring(0, 8)}"
    }

    @BeforeEach
    fun setUp() {
        withSurrealDB(config) {
            query<Unit>("DELETE test").getOrThrow()
            return@withSurrealDB
        }
    }

    @AfterEach
    fun tearDown() {
        // Optional cleanup if needed
    }

    @Test
    fun `any should store and retrieve mixed types`() {
        withSurrealDB(config) {
            val id = RecordId("test", uniqueId("any"))
            val anyString = TestAny(SurrealAny(JsonPrimitive("hello")))
            create(id, anyString).getOrThrow()
            val retrieved = select<TestAny>(id).getOrThrow().first()
            retrieved.data.value shouldBe JsonPrimitive("hello")

            val anyNumber = TestAny(SurrealAny(JsonPrimitive(42)))
            update(id, anyNumber).getOrThrow()
            val updated = select<TestAny>(id).getOrThrow().first()
            updated.data.value shouldBe JsonPrimitive(42)
            return@withSurrealDB
        }
    }

    @Test
    fun `array should handle typed items and max length`() {
        withSurrealDB(config) {
            val id = RecordId("test", uniqueId("array"))
            val array = TestArray(SurrealArray(listOf("a", "b", "c"), maxLength = 5))
            create(id, array).getOrThrow()
            val retrieved = select<TestArray<String>>(id).getOrThrow().first()
            retrieved.data.items shouldContainExactly listOf("a", "b", "c")
            retrieved.data.maxLength shouldBe 5
            return@withSurrealDB
        }
    }

    @Test
    fun `bool should store true or false`() {
        withSurrealDB(config) {
            val id = RecordId("test", uniqueId("bool"))
            create(id, TestBool(SurrealBool(true))).getOrThrow()
            val retrieved = select<TestBool>(id).getOrThrow().first()
            retrieved.data.value shouldBe true
            return@withSurrealDB
        }
    }

    @Test
    fun `bytes should store and retrieve byte array`() {
        withSurrealDB(config) {
            val id = RecordId("test", uniqueId("bytes"))
            val bytes = TestBytes(SurrealBytes("test".toByteArray()))
            create(id, bytes).getOrThrow()
            val retrieved = select<TestBytes>(id).getOrThrow().first()
            retrieved.data.value.contentEquals("test".toByteArray()) shouldBe true
            return@withSurrealDB
        }
    }

    @Test
    fun `datetime should store and retrieve ISO 8601 date`() {
        withSurrealDB(config) {
            val id = RecordId("test", uniqueId("datetime"))
            val now = OffsetDateTime.now()
            val expectedFormatted = now.format(SurrealDatetime.FORMATTER)
            create(id, TestDatetime(SurrealDatetime(now))).getOrThrow()
            val retrieved = select<TestDatetime>(id).getOrThrow().first()
            retrieved.data.value.format(SurrealDatetime.FORMATTER) shouldBe expectedFormatted
            return@withSurrealDB
        }
    }

    @Test
    fun `decimal should store and retrieve arbitrary precision number`() {
        withSurrealDB(config) {
            val id = RecordId("test", uniqueId("decimal"))
            val decimal = TestDecimal(SurrealDecimal(BigDecimal("123.456789")))
            create(id, decimal).getOrThrow()
            val retrieved = select<TestDecimal>(id).getOrThrow().first()
            retrieved.data.value shouldBe BigDecimal("123.456789")
            return@withSurrealDB
        }
    }

    @Test
    fun `duration should store and retrieve time length`() {
        withSurrealDB(config) {
            val id = RecordId("test", uniqueId("duration"))
            val duration = TestDuration(SurrealDuration("1w2d3h"))
            create(id, duration).getOrThrow()
            val retrieved = select<TestDuration>(id).getOrThrow().first()
            retrieved.data.value shouldBe "1w2d3h"
            return@withSurrealDB
        }
    }

    @Test
    fun `float should store and retrieve 64-bit float`() {
        withSurrealDB(config) {
            val id = RecordId("test", uniqueId("float"))
            val float = TestFloat(SurrealFloat(3.14))
            create(id, float).getOrThrow()
            val retrieved = select<TestFloat>(id).getOrThrow().first()
            retrieved.data.value shouldBe 3.14
            return@withSurrealDB
        }
    }

    @Test
    fun `geometry point should store and retrieve coordinates`() {
        withSurrealDB(config) {
            val id = RecordId("test", uniqueId("geometry"))
            val point = TestGeometry(SurrealGeometry.Point(listOf(10.0, 20.0)))
            create(id, point).getOrThrow()
            val retrieved = select<TestGeometry>(id).getOrThrow().first()
            retrieved.data.shouldBeInstanceOf<SurrealGeometry.Point>()
            (retrieved.data as SurrealGeometry.Point).coordinates shouldContainExactly listOf(10.0, 20.0)
            return@withSurrealDB
        }
    }

    @Test
    fun `geometry linestring should store and retrieve coordinates`() {
        withSurrealDB(config) {
            val id = RecordId("test", uniqueId("geometry"))
            val linestring = TestGeometry(SurrealGeometry.LineString(listOf(listOf(10.0, 20.0), listOf(30.0, 40.0))))
            create(id, linestring).getOrThrow()
            val retrieved = select<TestGeometry>(id).getOrThrow().first()
            retrieved.data.shouldBeInstanceOf<SurrealGeometry.LineString>()
            (retrieved.data as SurrealGeometry.LineString).coordinates shouldContainExactly listOf(listOf(10.0, 20.0), listOf(30.0, 40.0))
            return@withSurrealDB
        }
    }

    @Test
    fun `int should store and retrieve 64-bit integer`() {
        withSurrealDB(config) {
            val id = RecordId("test", uniqueId("int"))
            val int = TestInt(SurrealInt(1234567890L))
            create(id, int).getOrThrow()
            val retrieved = select<TestInt>(id).getOrThrow().first()
            retrieved.data.value shouldBe 1234567890L
            return@withSurrealDB
        }
    }

    @Test
    fun `number should store and retrieve various number types`() {
        withSurrealDB(config) {
            val id = RecordId("test", uniqueId("number"))
            val number = TestNumber(SurrealNumber(42))
            create(id, number).getOrThrow()
            val retrieved = select<TestNumber>(id).getOrThrow().first()
            retrieved.data.value.toInt() shouldBe 42

            val decimalNumber = TestNumber(SurrealNumber(BigDecimal("123.45")))
            update(id, decimalNumber).getOrThrow()
            val updated = select<TestNumber>(id).getOrThrow().first()
            updated.data.value shouldBe BigDecimal("123.45")
            return@withSurrealDB
        }
    }

    @Test
    fun `object should store and retrieve nested data`() {
        withSurrealDB(config) {
            val id = RecordId("test", uniqueId("object"))
            val obj = TestObject(SurrealObject(mapOf("key" to JsonPrimitive("value"))))
            create(id, obj).getOrThrow()
            val retrieved = select<TestObject>(id).getOrThrow().first()
            retrieved.data.value["key"] shouldBe JsonPrimitive("value")
            return@withSurrealDB
        }
    }

    @Test
    fun `literal should store and retrieve union-like value`() {
        withSurrealDB(config) {
            val id = RecordId("test", uniqueId("literal"))
            val literal = TestLiteral(SurrealLiteral(JsonPrimitive("a")))
            create(id, literal).getOrThrow()
            val retrieved = select<TestLiteral>(id).getOrThrow().first()
            retrieved.data.value shouldBe JsonPrimitive("a")
            return@withSurrealDB
        }
    }

    @Test
    fun `option should store and retrieve optional value`() {
        withSurrealDB(config) {
            val id = RecordId("test", uniqueId("option"))
            val option = TestOption(SurrealOption<Int>(42))
            create(id, option).getOrThrow()
            val retrieved = select<TestOption<Int>>(id).getOrThrow().first()
            retrieved.data.value shouldBe 42

            val nullOption = TestOption(SurrealOption<Int>(null))
            update(id, nullOption).getOrThrow()
            val updated = select<TestOption<Int>>(id).getOrThrow().first()
            updated.data.value shouldBe null
            return@withSurrealDB
        }
    }

    @Test
    fun `range should store and retrieve int bounded range`() {
        withSurrealDB(config) {
            val id = RecordId("test", uniqueId("range"))
            val range = TestRange(SurrealRange(0, 10, true))
            create(id, range).getOrThrow()
            val retrieved = select<TestRange<Int>>(id).getOrThrow().first()
            retrieved.data.start shouldBe 0
            retrieved.data.end shouldBe 10
            retrieved.data.inclusiveEnd shouldBe true
            return@withSurrealDB
        }
    }

    @Test
    fun `range should store and retrieve string bounded range`() {
        withSurrealDB(config) {
            val id = RecordId("test", uniqueId("range"))
            val range = TestRange(SurrealRange("a", "z", false))
            create(id, range).getOrThrow()
            val retrieved = select<TestRange<String>>(id).getOrThrow().first()
            retrieved.data.start shouldBe "a"
            retrieved.data.end shouldBe "z"
            retrieved.data.inclusiveEnd shouldBe false
            return@withSurrealDB
        }
    }

    @Test
    fun `record should store and retrieve reference`() {
        withSurrealDB(config) {
            val refId = RecordId("test", uniqueId("ref"))
            create(refId, TestString(SurrealString("ref"))).getOrThrow()
            val id = RecordId("test", uniqueId("record"))
            val record = TestRecord(SurrealRecord(refId))
            create(id, record).getOrThrow()
            val retrieved = select<TestRecord>(id).getOrThrow().first()
            retrieved.data.recordId shouldBe refId
            return@withSurrealDB
        }
    }

    @Test
    fun `set should store and retrieve unique items`() {
        withSurrealDB(config) {
            val id = RecordId("test", uniqueId("set"))
            val set = TestSet(SurrealSet(setOf("a", "b", "a"), maxLength = 5))
            create(id, set).getOrThrow()
            val retrieved = select<TestSet<String>>(id).getOrThrow().first()
            retrieved.data.items shouldContainExactly setOf("a", "b")
            retrieved.data.maxLength shouldBe 5
            return@withSurrealDB
        }
    }

    @Test
    fun `string should store and retrieve text`() {
        withSurrealDB(config) {
            val id = RecordId("test", uniqueId("string"))
            val string = TestString(SurrealString("hello"))
            create(id, string).getOrThrow()
            val retrieved = select<TestString>(id).getOrThrow().first()
            retrieved.data.value shouldBe "hello"
            return@withSurrealDB
        }
    }
}