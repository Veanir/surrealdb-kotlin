package pl.steclab.surrealdb

import kotlinx.serialization.*
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.*
import java.math.BigDecimal
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.*

interface SurrealType

@Serializable(with = AnySerializer::class)
data class SurrealAny(val value: JsonElement) : SurrealType

object AnySerializer : KSerializer<SurrealAny> {
    override val descriptor = PrimitiveSerialDescriptor("SurrealAny", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: SurrealAny) = encoder.encodeSerializableValue(JsonElement.serializer(), value.value)
    override fun deserialize(decoder: Decoder): SurrealAny = SurrealAny(decoder.decodeSerializableValue(JsonElement.serializer()))
}

// Array: No custom serializer needed, rely on default serialization
@Serializable
data class SurrealArray<T>(
    val items: List<T>,
    val maxLength: Int? = null
) : SurrealType {
    init {
        maxLength?.let { require(items.size <= it) { "Array exceeds max length $maxLength" } }
    }
}

@Serializable
data class SurrealBool(val value: Boolean) : SurrealType

@Serializable(with = BytesSerializer::class)
data class SurrealBytes(val value: ByteArray) : SurrealType {
    override fun equals(other: Any?): Boolean = other is SurrealBytes && value.contentEquals(other.value)
    override fun hashCode(): Int = value.contentHashCode()
}

object BytesSerializer : KSerializer<SurrealBytes> {
    override val descriptor = PrimitiveSerialDescriptor("SurrealBytes", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: SurrealBytes) = encoder.encodeString(Base64.getEncoder().encodeToString(value.value))
    override fun deserialize(decoder: Decoder): SurrealBytes = SurrealBytes(Base64.getDecoder().decode(decoder.decodeString()))
}

@Serializable(with = DatetimeSerializer::class)
data class SurrealDatetime(val value: OffsetDateTime) : SurrealType {
    companion object {
        val FORMATTER: DateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME
    }
}

object DatetimeSerializer : KSerializer<SurrealDatetime> {
    override val descriptor = PrimitiveSerialDescriptor("SurrealDatetime", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: SurrealDatetime) {
        val formatted = value.value.format(SurrealDatetime.FORMATTER)
        encoder.encodeString("d\"$formatted\"") // e.g., d"2025-03-08T20:41:03.401851Z"
    }
    override fun deserialize(decoder: Decoder): SurrealDatetime {
        val string = decoder.decodeString()
        val cleanedString = string.removePrefix("d\"").removeSuffix("\"")
        return SurrealDatetime(OffsetDateTime.parse(cleanedString, SurrealDatetime.FORMATTER))
    }
}

@Serializable(with = DecimalSerializer::class)
data class SurrealDecimal(val value: BigDecimal) : SurrealType

object DecimalSerializer : KSerializer<SurrealDecimal> {
    override val descriptor = PrimitiveSerialDescriptor("SurrealDecimal", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: SurrealDecimal) = encoder.encodeString(value.value.toString())
    override fun deserialize(decoder: Decoder): SurrealDecimal = SurrealDecimal(BigDecimal(decoder.decodeString()))
}

@Serializable(with = DurationSerializer::class)
data class SurrealDuration(val value: String) : SurrealType {
    init {
        require(value.matches(Regex("""^(\d+[wdhmsu])+|\d+$"""))) { "Invalid duration format: $value" }
    }
}

object DurationSerializer : KSerializer<SurrealDuration> {
    override val descriptor = PrimitiveSerialDescriptor("SurrealDuration", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: SurrealDuration) = encoder.encodeString(value.value)
    override fun deserialize(decoder: Decoder): SurrealDuration = SurrealDuration(decoder.decodeString())
}

@Serializable
data class SurrealFloat(val value: Double) : SurrealType

@Serializable
sealed class SurrealGeometry : SurrealType {
    @Serializable
    data class Point(val coordinates: List<Double>) : SurrealGeometry() {
        init { require(coordinates.size == 2) { "Point requires [longitude, latitude]" } }
    }
    @Serializable
    data class LineString(val coordinates: List<List<Double>>) : SurrealGeometry() {
        init { require(coordinates.all { it.size == 2 }) { "LineString coordinates must be [longitude, latitude]" } }
    }
    @Serializable
    data class Polygon(val coordinates: List<List<List<Double>>>) : SurrealGeometry() {
        init { require(coordinates.all { ring -> ring.all { it.size == 2 } }) { "Polygon coordinates must be [longitude, latitude]" } }
    }
    @Serializable
    data class MultiPoint(val coordinates: List<List<Double>>) : SurrealGeometry() {
        init { require(coordinates.all { it.size == 2 }) { "MultiPoint coordinates must be [longitude, latitude]" } }
    }
}

@Serializable
data class SurrealInt(val value: Long) : SurrealType

@Serializable(with = NumberSerializer::class)
data class SurrealNumber(val value: Number) : SurrealType

object NumberSerializer : KSerializer<SurrealNumber> {
    override val descriptor = PrimitiveSerialDescriptor("SurrealNumber", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: SurrealNumber) = when (value.value) {
        is Int -> encoder.encodeInt(value.value.toInt())
        is Long -> encoder.encodeLong(value.value.toLong())
        is Float -> encoder.encodeFloat(value.value.toFloat())
        is Double -> encoder.encodeDouble(value.value.toDouble())
        is BigDecimal -> encoder.encodeString(value.value.toString())
        else -> throw SerializationException("Unsupported number type: ${value.value}")
    }
    override fun deserialize(decoder: Decoder): SurrealNumber = try {
        SurrealNumber(decoder.decodeInt().toLong())
    } catch (e: Exception) {
        try {
            SurrealNumber(decoder.decodeDouble())
        } catch (e: Exception) {
            SurrealNumber(BigDecimal(decoder.decodeString()))
        }
    }
}

@Serializable
data class SurrealObject(val value: Map<String, JsonElement>) : SurrealType

@Serializable(with = LiteralSerializer::class)
data class SurrealLiteral(val value: JsonElement) : SurrealType

object LiteralSerializer : KSerializer<SurrealLiteral> {
    override val descriptor = PrimitiveSerialDescriptor("SurrealLiteral", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: SurrealLiteral) = encoder.encodeSerializableValue(JsonElement.serializer(), value.value)
    override fun deserialize(decoder: Decoder): SurrealLiteral = SurrealLiteral(decoder.decodeSerializableValue(JsonElement.serializer()))
}

// Option: No custom serializer, rely on default
@Serializable
data class SurrealOption<T>(val value: T?) : SurrealType

// Range: Custom serializer moved here
@Serializable(with = SurrealRangeSerializer::class)
data class SurrealRange<T : Comparable<T>>(
    val start: T?,
    val end: T?,
    val inclusiveEnd: Boolean = false
) : SurrealType {
    init {
        if (start != null && end != null) {
            require(start <= end) { "Start must be less than or equal to end" }
        }
    }
}

object SurrealRangeSerializer : KSerializer<SurrealRange<Comparable<Any>>> {
    override val descriptor = PrimitiveSerialDescriptor("SurrealRange", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: SurrealRange<Comparable<Any>>) {
        val str = when {
            value.start == null && value.end == null -> ".."
            value.start == null -> "..${if (value.inclusiveEnd) "=" else ""}${value.end}"
            value.end == null -> "${value.start}.."
            else -> "${value.start}..${if (value.inclusiveEnd) "=" else ""}${value.end}"
        }
        encoder.encodeString(str)
    }
    @Suppress("UNCHECKED_CAST")
    override fun deserialize(decoder: Decoder): SurrealRange<Comparable<Any>> {
        val str = decoder.decodeString()
        val parts = str.split("..")
        return when {
            str == ".." -> SurrealRange(null, null)
            str.startsWith("..") -> {
                val endStr = str.removePrefix("..").removePrefix("=")
                val end = endStr.toIntOrNull() ?: endStr
                SurrealRange(null, end as Comparable<Any>, str.contains("="))
            }
            str.endsWith("..") -> {
                val startStr = str.removeSuffix("..")
                val start = startStr.toIntOrNull() ?: startStr
                SurrealRange(start as Comparable<Any>, null)
            }
            else -> {
                val (startStr, endPart) = parts
                val inclusive = endPart.startsWith("=")
                val endStr = if (inclusive) endPart.removePrefix("=") else endPart
                val start = startStr.toIntOrNull() ?: startStr
                val end = endStr.toIntOrNull() ?: endStr
                SurrealRange(start as Comparable<Any>, end as Comparable<Any>, inclusive)
            }
        }
    }
}

@Serializable(with = SurrealRecordSerializer::class)
data class SurrealRecord(val recordId: RecordId) : SurrealType {
    override fun toString(): String = recordId.toString()
}

object SurrealRecordSerializer : KSerializer<SurrealRecord> {
    override val descriptor = PrimitiveSerialDescriptor("SurrealRecord", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: SurrealRecord) = encoder.encodeString(value.recordId.toString())
    override fun deserialize(decoder: Decoder): SurrealRecord = SurrealRecord(RecordId.parse(decoder.decodeString()))
}

// Set: No custom serializer, rely on default
@Serializable
data class SurrealSet<T>(
    val items: Set<T>,
    val maxLength: Int? = null
) : SurrealType {
    init {
        maxLength?.let { require(items.size <= it) { "Set exceeds max length $maxLength" } }
    }
}

@Serializable
data class SurrealString(val value: String) : SurrealType

// Test utility types (no custom serializers needed)
@Serializable
data class TestAny(val data: SurrealAny)

@Serializable
data class TestArray<T>(val data: SurrealArray<T>)

@Serializable
data class TestBool(val data: SurrealBool)

@Serializable
data class TestBytes(val data: SurrealBytes)

@Serializable
data class TestDatetime(val data: SurrealDatetime)

@Serializable
data class TestDecimal(val data: SurrealDecimal)

@Serializable
data class TestDuration(val data: SurrealDuration)

@Serializable
data class TestFloat(val data: SurrealFloat)

@Serializable
data class TestGeometry(val data: SurrealGeometry)

@Serializable
data class TestInt(val data: SurrealInt)

@Serializable
data class TestNumber(val data: SurrealNumber)

@Serializable
data class TestObject(val data: SurrealObject)

@Serializable
data class TestLiteral(val data: SurrealLiteral)

@Serializable
data class TestOption<T>(val data: SurrealOption<T>)

@Serializable
data class TestRange<T : Comparable<T>>(val data: SurrealRange<T>)

@Serializable
data class TestRecord(val data: SurrealRecord)

@Serializable
data class TestSet<T>(val data: SurrealSet<T>)

@Serializable
data class TestString(val data: SurrealString)