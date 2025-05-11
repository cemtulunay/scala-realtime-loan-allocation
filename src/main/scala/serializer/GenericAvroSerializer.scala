package serializer

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

import java.io.ByteArrayOutputStream

/**
 * A generic Avro serializer for Kafka that can serialize any type of record.
 *
 * @param schemaString The Avro schema as a string
 * @param topic The Kafka topic to produce to
 * @param toGenericRecord Function to convert a record of type T to a GenericRecord
 * @param keyExtractor Optional function to extract a key from the record
 * @tparam T The type of records to serialize
 */
// Generic Avro serializer
class GenericAvroSerializer[T](
                                schemaString: String,
                                topic: String,
                                toGenericRecord: (T, GenericRecord) => Unit,
                                keyExtractor: Option[T => Array[Byte]] = None
                              ) extends KafkaSerializationSchema[T] with Serializable {

  @transient private lazy val schema: Schema = new Schema.Parser().parse(schemaString)

  override def serialize(element: T, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().blockingBinaryEncoder(byteArrayOutputStream, null)
    val writer = new GenericDatumWriter[GenericRecord](schema)

    val record = new GenericData.Record(schema)
    toGenericRecord(element, record)

    writer.write(record, encoder)
    encoder.flush()

    val avroBytes = byteArrayOutputStream.toByteArray
    byteArrayOutputStream.close()

    val key = keyExtractor.map(_(element)).orNull

    new ProducerRecord[Array[Byte], Array[Byte]](
      topic,
      key,
      avroBytes
    )
  }
}
