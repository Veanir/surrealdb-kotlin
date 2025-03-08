package pl.steclab.surrealdb.model

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive

@Serializable(with = RecordIdSerializer::class)
data class RecordId(val table: String, val id: String) {
    constructor(table: String, id: JsonElement) : this(
        table,
        when (id) {
            is JsonPrimitive -> id.content
            else -> id.toString()
        }
    )

    override fun toString(): String = "$table:$id"

    companion object {
        fun parse(thing: String): RecordId {
            val parts = thing.split(":", limit = 2)
            require(parts.size == 2) { "Invalid record ID: $thing (expected table:id)" }
            return RecordId(parts[0], parts[1])
        }
    }
}

object RecordIdSerializer : KSerializer<RecordId> {
    override val descriptor = PrimitiveSerialDescriptor("RecordId", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: RecordId) {
        encoder.encodeString(value.toString())
    }

    override fun deserialize(decoder: Decoder): RecordId {
        return RecordId.parse(decoder.decodeString())
    }
}