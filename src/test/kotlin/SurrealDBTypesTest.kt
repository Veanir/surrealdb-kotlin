package pl.steclab.surrealdb

import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import pl.steclab.surrealdb.client.SurrealDBClientConfig
import pl.steclab.surrealdb.model.RecordId
import pl.steclab.surrealdb.model.SignInParams
import pl.steclab.surrealdb.result.getOrThrow
import pl.steclab.surrealdb.utils.withSurrealDB
import java.math.BigDecimal
import java.time.OffsetDateTime
import java.time.ZoneOffset
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

        // Schema definitions for each type
        private val schemaDefinitions = """
            DEFINE TABLE any_test SCHEMAFULL;
            DEFINE FIELD data ON any_test TYPE any;

            DEFINE TABLE array_test SCHEMAFULL;
            DEFINE FIELD data ON array_test TYPE array<string, 5>;

            DEFINE TABLE bool_test SCHEMAFULL;
            DEFINE FIELD data ON bool_test TYPE bool;

            DEFINE TABLE bytes_test SCHEMAFULL;
            DEFINE FIELD data ON bytes_test TYPE bytes;

            DEFINE TABLE datetime_test SCHEMAFULL;
            DEFINE FIELD data ON datetime_test TYPE datetime;

            DEFINE TABLE decimal_test SCHEMAFULL;
            DEFINE FIELD data ON decimal_test TYPE decimal;

            DEFINE TABLE duration_test SCHEMAFULL;
            DEFINE FIELD data ON duration_test TYPE duration;

            DEFINE TABLE float_test SCHEMAFULL;
            DEFINE FIELD data ON float_test TYPE float;

            DEFINE TABLE geometry_point_test SCHEMAFULL;
            DEFINE FIELD data ON geometry_point_test TYPE geometry<point>;

            DEFINE TABLE geometry_line_test SCHEMAFULL;
            DEFINE FIELD data ON geometry_line_test TYPE geometry<line>;

            DEFINE TABLE int_test SCHEMAFULL;
            DEFINE FIELD data ON int_test TYPE int;

            DEFINE TABLE number_test SCHEMAFULL;
            DEFINE FIELD data ON number_test TYPE number;

            DEFINE TABLE object_test SCHEMAFULL;
            DEFINE FIELD data ON object_test TYPE object;

            DEFINE TABLE literal_test SCHEMAFULL;
            DEFINE FIELD data ON literal_test TYPE 'success' | 42 | { status: string };

            DEFINE TABLE option_test SCHEMAFULL;
            DEFINE FIELD data ON option_test TYPE option<int>;

            DEFINE TABLE range_int_test SCHEMAFULL;
            DEFINE FIELD data ON range_int_test TYPE string;

            DEFINE TABLE range_string_test SCHEMAFULL;
            DEFINE FIELD data ON range_string_test TYPE string;

            DEFINE TABLE record_test SCHEMAFULL;
            DEFINE FIELD data ON record_test TYPE record<string_test>;

            DEFINE TABLE set_test SCHEMAFULL;
            DEFINE FIELD data ON set_test TYPE set<string, 5>;

            DEFINE TABLE string_test SCHEMAFULL;
            DEFINE FIELD data ON string_test TYPE string;
        """.trimIndent()

        private val tableNames = listOf(
            "any_test", "array_test", "bool_test", "bytes_test", "datetime_test",
            "decimal_test", "duration_test", "float_test", "geometry_point_test",
            "geometry_line_test", "int_test", "number_test", "object_test",
            "literal_test", "option_test", "range_int_test", "range_string_test",
            "record_test", "set_test", "string_test"
        )
    }

    @BeforeEach
    fun setUp() {
        withSurrealDB(config) {
            val removeQueries = tableNames.joinToString("; ") { "REMOVE TABLE IF EXISTS $it" }
            query<Unit>(removeQueries).getOrThrow()
            query<Unit>(schemaDefinitions).getOrThrow()
            return@withSurrealDB
        }
    }

    @AfterEach
    fun tearDown() {
        withSurrealDB(config) {
            val deleteQueries = tableNames.joinToString("; ") { "DELETE $it" }
            query<Unit>(deleteQueries).getOrThrow()
            return@withSurrealDB
        }
    }

    @Test
    fun `any should store and retrieve mixed types`() {
        withSurrealDB(config) {
            val id = RecordId("any_test", uniqueId("any"))
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
            val id = RecordId("array_test", uniqueId("array"))
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
            val id = RecordId("bool_test", uniqueId("bool"))
            create(id, TestBool(SurrealBool(true))).getOrThrow()
            val retrieved = select<TestBool>(id).getOrThrow().first()
            retrieved.data.value shouldBe true
            return@withSurrealDB
        }
    }

    @Test
    fun `bytes should store and retrieve byte array`() {
        withSurrealDB(config) {
            val id = RecordId("bytes_test", uniqueId("bytes"))
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
            val id = RecordId("datetime_test", uniqueId("datetime"))
            val now = OffsetDateTime.now()
            val expectedUtcDateTime = now.withOffsetSameInstant(ZoneOffset.UTC)
            create(id, TestDatetime(SurrealDatetime(now))).getOrThrow()
            val retrieved = select<TestDatetime>(id).getOrThrow().first()

            retrieved.data.value shouldBe expectedUtcDateTime
            return@withSurrealDB
        }
    }

    @Test
    fun `decimal should store and retrieve arbitrary precision number`() {
        withSurrealDB(config) {
            val id = RecordId("decimal_test", uniqueId("decimal"))
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
            val id = RecordId("duration_test", uniqueId("duration"))
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
            val id = RecordId("float_test", uniqueId("float"))
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
            val id = RecordId("geometry_point_test", uniqueId("geometry"))
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
            val id = RecordId("geometry_line_test", uniqueId("geometry"))
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
            val id = RecordId("int_test", uniqueId("int"))
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
            val id = RecordId("number_test", uniqueId("number"))
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
            val id = RecordId("object_test", uniqueId("object"))
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
            val id = RecordId("literal_test", uniqueId("literal"))
            val literalString = TestLiteral(SurrealLiteral(JsonPrimitive("success")))
            create(id, literalString).getOrThrow()
            val retrievedString = select<TestLiteral>(id).getOrThrow().first()
            retrievedString.data.value shouldBe JsonPrimitive("success")

            val literalNumber = TestLiteral(SurrealLiteral(JsonPrimitive(42)))
            update(id, literalNumber).getOrThrow()
            val retrievedNumber = select<TestLiteral>(id).getOrThrow().first()
            retrievedNumber.data.value shouldBe JsonPrimitive(42)

            val literalObject = TestLiteral(SurrealLiteral(buildJsonObject { put("status", "active") }))
            update(id, literalObject).getOrThrow()
            val retrievedObject = select<TestLiteral>(id).getOrThrow().first()
            retrievedObject.data.value shouldBe buildJsonObject { put("status", "active") }
            return@withSurrealDB
        }
    }

    @Test
    fun `option should store and retrieve optional value`() {
        withSurrealDB(config) {
            val id = RecordId("option_test", uniqueId("option"))
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
            val id = RecordId("range_int_test", uniqueId("range"))
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
            val id = RecordId("range_string_test", uniqueId("range"))
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
            val refId = RecordId("string_test", uniqueId("ref"))
            create(refId, TestString(SurrealString("ref"))).getOrThrow()
            val id = RecordId("record_test", uniqueId("record"))
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
            val id = RecordId("set_test", uniqueId("set"))
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
            val id = RecordId("string_test", uniqueId("string"))
            val string = TestString(SurrealString("hello"))
            create(id, string).getOrThrow()
            val retrieved = select<TestString>(id).getOrThrow().first()
            retrieved.data.value shouldBe "hello"
            return@withSurrealDB
        }
    }
}