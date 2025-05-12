package serializer

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

/**
 * Custom Kryo serializer for Avro GenericRecord objects.
 * Enables efficient serialization/deserialization of Avro records for Flink's state backend.
 *
 * This serializer:
 * 1. Preserves the complete schema information along with the data
 * 2. Handles field-by-field serialization with proper type handling
 * 3. Manages null values safely within the record structure
 * 4. Enables Flink to checkpoint/restore GenericRecord objects in stateful operations
 *
 * Kryo serialization is used for Flink's internal state management, allowing Avro records
 * to be stored efficiently when using features like keyed state, windows, or checkpointing.
 */

// Custom Kryo Serializer for GenericRecord
class GenericRecordKryoSerializer extends Serializer[GenericRecord] {
  override def write(kryo: Kryo, output: Output, record: GenericRecord): Unit = {
    // Serialize schema
    val schemaString = record.getSchema.toString
    output.writeString(schemaString)

    // Serialize field count
    val fields = record.getSchema.getFields
    output.writeInt(fields.size)

    // Serialize each field name and value
    fields.forEach(field => {
      val fieldName = field.name()
      val value = record.get(fieldName)

      output.writeString(fieldName)

      // Handle potential null values
      if (value == null) {
        output.writeBoolean(false)
      } else {
        output.writeBoolean(true)
        kryo.writeClassAndObject(output, value)
      }
    })
  }

  override def read(kryo: Kryo, input: Input, recordClass: Class[GenericRecord]): GenericRecord = {
    // Read schema
    val schemaString = input.readString()
    val schema = new Schema.Parser().parse(schemaString)

    // Create a new record
    val record = new GenericData.Record(schema)

    // Read field count
    val fieldCount = input.readInt()

    // Deserialize and set each field
    (0 until fieldCount).foreach(_ => {
      val fieldName = input.readString()

      // Check if value is present
      val hasValue = input.readBoolean()
      if (hasValue) {
        val value = kryo.readClassAndObject(input)
        record.put(fieldName, value)
      }
    })

    record
  }
}