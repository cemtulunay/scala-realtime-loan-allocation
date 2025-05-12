package serializer

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{DatumReader, DecoderFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.io.ByteArrayInputStream

/**
 * Deserializer for converting Kafka binary messages into Avro GenericRecord objects.
 * Implements Flink's KafkaDeserializationSchema interface to enable consumption of
 * Avro-serialized data from Kafka topics.
 *
 * @param schema The Avro schema used to deserialize the binary data
 *
 * This class:
 * 1. Takes binary data from Kafka records
 * 2. Uses Avro's deserialization mechanism to convert bytes to structured records
 * 3. Provides error handling for deserialization failures
 * 4. Enables type information for Flink's type system
 */


// Generic Avro Kafka Deserialization Schema
class GenericAvroDeserializer(schema: Schema) extends KafkaDeserializationSchema[GenericRecord] {
  override def isEndOfStream(nextElement: GenericRecord): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): GenericRecord = {
    try {
      val bytes = record.value()

      val reader: DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord](schema)
      val decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bytes), null)

      val genericRecord = reader.read(null, decoder)

      if (genericRecord == null) {
        throw new IllegalStateException("Deserialized record is null")
      }

      genericRecord
    } catch {
      case e: Exception =>
        println(s"Deserialization error: ${e.getMessage}")
        e.printStackTrace()
        throw e
    }
  }

  override def getProducedType(): TypeInformation[GenericRecord] =
    TypeExtractor.getForClass(classOf[GenericRecord])
}