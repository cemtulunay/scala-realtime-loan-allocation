package loan

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.{DatumReader, DecoderFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.io.ByteArrayInputStream
import java.util.Properties

object incomePredictionService_20240402_2100_Avro {

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

  def incomePredictionRequestConsumer(): Unit = {
    // 1-) Setup Environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Register custom Kryo serializer
    env.getConfig.addDefaultKryoSerializer(
      classOf[GenericRecord],
      classOf[GenericRecordKryoSerializer]
    )

    // Ensure generic types are supported
    //env.getConfig.setGenericTypes(true)

    // 2-) Define Avro schema
    val schemaString =
      """
        |{
        |  "type": "record",
        |  "name": "IncomePredictionRequest",
        |  "namespace": "loan",
        |  "fields": [
        |    {"name": "requestId", "type": ["string"]},
        |    {"name": "applicationId", "type": ["string"]},
        |    {"name": "customerId", "type": ["int"]},
        |    {"name": "prospectId", "type": ["int"]},
        |    {"name": "requestedAt", "type": ["long"]},
        |    {"name": "incomeSource", "type": ["null","string"], "default": null},
        |    {"name": "isCustomer", "type": ["null","boolean"], "default": null}
        |  ]
        |}
    """.stripMargin
    val schema = new Schema.Parser().parse(schemaString)

    // 3-) Configure Kafka properties
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProps.setProperty("group.id", "income_prediction_request_consumer")

    // 4-) Create custom Avro deserialization schema
    val avroDeserializer = new KafkaDeserializationSchema[GenericRecord] {
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

    // 5-) Create Kafka consumer with Avro deserializer
    val kafkaConsumer = new FlinkKafkaConsumer[GenericRecord](
      "income_prediction_request",
      avroDeserializer,
      kafkaProps
    )

    kafkaConsumer.setStartFromEarliest()

    // 6-) Add source to Flink environment and process
    val stream = env.addSource(kafkaConsumer)

    // 7-) Process the Avro records
    stream.map(record => {
        try {
          val requestId = record.get("requestId").toString
          val applicationId = record.get("applicationId").toString
          val customerId = record.get("customerId").toString
          val prospectId = record.get("prospectId").toString
          val requestedAt = record.get("requestedAt").toString
          val incomeSource = record.get("incomeSource").toString
          val isCustomer = record.get("isCustomer").toString
          s"requestId: $requestId, applicationId: $applicationId, customerId: $customerId, prospectId: $prospectId, requestedAt: $requestedAt, incomeSource: $incomeSource, isCustomer: $isCustomer"
        } catch {
          case e: Exception =>
            println(s"Error processing record: ${e.getMessage}")
            e.printStackTrace()
            "Error"
        }
      })
      .print()

    // 8-) Execute Flink job
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    incomePredictionRequestConsumer()
  }
}