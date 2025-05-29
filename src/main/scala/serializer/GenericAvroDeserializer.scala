package serializer

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{DatumReader, DecoderFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory

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
//  override def isEndOfStream(nextElement: GenericRecord): Boolean = false
//
//  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): GenericRecord = {
//    try {
//      val bytes = record.value()
//
//      val reader: DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord](schema)
//      val decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bytes), null)
//
//      val genericRecord = reader.read(null, decoder)
//
//      if (genericRecord == null) {
//        throw new IllegalStateException("Deserialized record is null")
//      }
//
//      genericRecord
//    } catch {
//      case e: Exception =>
//        println(s"Deserialization error: ${e.getMessage}")
//        e.printStackTrace()
//        throw e
//    }
//  }
//
//  override def getProducedType(): TypeInformation[GenericRecord] =
//    TypeExtractor.getForClass(classOf[GenericRecord])
// Add a logger
private val logger = LoggerFactory.getLogger(classOf[GenericAvroDeserializer])

  override def isEndOfStream(nextElement: GenericRecord): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): GenericRecord = {
    try {

      println(s"CRITICAL_GenericAvroDeserializer: Received record - Topic: ${record.topic()}, Partition: ${record.partition()}, Offset: ${record.offset()}")
      println(s"CRITICAL_GenericAvroDeserializer: Record value bytes length: ${record.value().length}")

      // Log incoming record details
      logger.debug(s"CRITICAL_GenericAvroDeserializer: Deserializing record: topic=${record.topic()}, partition=${record.partition()}, offset=${record.offset()}")

      val bytes = record.value()

      // Log byte array size for debugging
      logger.trace(s"CRITICAL_GenericAvroDeserializer: Record byte array size: ${bytes.length} bytes")

      val reader: DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord](schema)
      //val decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bytes), null)
      val decoder = DecoderFactory.get().binaryDecoder(bytes, null)

      val genericRecord = reader.read(null, decoder)

      if (genericRecord == null) {
        logger.error(s"CRITICAL_GenericAvroDeserializer: Deserialized record is null for topic=${record.topic()}, partition=${record.partition()}, offset=${record.offset()}")
        println("CRITICAL_GenericAvroDeserializer: Deserialized record is null")
        throw new IllegalStateException("CRITICAL_GenericAvroDeserializer: Deserialized record is null")
      }

      // Log successful deserialization
      logger.debug(s"CRITICAL_GenericAvroDeserializer: Successfully deserialized record from topic=${record.topic()}")
      println(s"CRITICAL_GenericAvroDeserializer: Successfully deserialized record: $genericRecord")

      genericRecord
    } catch {
      case e: Exception =>
        logger.error(s"CRITICAL_GenericAvroDeserializer: Deserialization error for topic=${record.topic()}, partition=${record.partition()}, offset=${record.offset()}: ${e.getMessage}", e)
        println(s"CRITICAL_GenericAvroDeserializer: Deserialization error: ${e.getMessage}")
        throw e
    }
  }

  override def getProducedType(): TypeInformation[GenericRecord] =
    TypeExtractor.getForClass(classOf[GenericRecord])

}