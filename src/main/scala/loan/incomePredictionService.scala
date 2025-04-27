package loan

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.{DatumReader, DecoderFactory}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.sql.PreparedStatement
import java.util.Properties

object incomePredictionService {

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

    // Enable checkpointing for reliability
    env.enableCheckpointing(10000)

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

    // 7-) Define case class for postgreSql format
    case class PredictionRecord(
                                 requestId: String,
                                 applicationId: String,
                                 customerId: Int,
                                 prospectId: Int,
                                 requestedAt: Long,
                                 incomeSource: String,
                                 isCustomer: Boolean,
                                 predictedIncome: Double
                               ) extends Serializable


    // 8-) Create a serializable function object for the mapping operation
    class RecordMapper extends MapFunction[GenericRecord, PredictionRecord] with Serializable {
      override def map(record: GenericRecord): PredictionRecord = {
        try {
          val customerId = record.get("customerId").toString.toInt
          PredictionRecord(
            requestId = record.get("requestId").toString,
            applicationId = record.get("applicationId").toString,
            customerId = customerId,
            prospectId = record.get("prospectId").toString.toInt,
            requestedAt = record.get("requestedAt").toString.toLong,
            incomeSource = Option(record.get("incomeSource")).map(_.toString).orNull,
            isCustomer = Option(record.get("isCustomer")).exists(_.toString.toBoolean),
            predictedIncome = 50000.0 + (customerId % 10) * 5000.0
          )
        } catch {
          case e: Exception =>
            println(s"Error processing record: ${e.getMessage}")
            e.printStackTrace()
            throw e
        }
      }
    }


    // 9-) Process the Avro records and make income predictions
    val processedStream = stream.map(new RecordMapper())

    // 10-) Create a serializable statement builder class
    class PredictionRecordJdbcBuilder extends JdbcStatementBuilder[PredictionRecord] with Serializable {
      override def accept(statement: PreparedStatement, record: PredictionRecord): Unit = {
        statement.setString(1, record.requestId)
        statement.setString(2, record.applicationId)
        statement.setInt(3, record.customerId)
        statement.setInt(4, record.prospectId)
        statement.setLong(5, record.requestedAt)
        statement.setString(6, record.incomeSource)
        statement.setBoolean(7, record.isCustomer)
        statement.setDouble(8, record.predictedIncome)
      }
    }

    // 11-) Save to PostgreSQL Serialized Data
    processedStream.addSink(
      JdbcSink.sink[PredictionRecord](
        // SQL statement
        """
        INSERT INTO income_predictions (
          request_id, application_id, customer_id, prospect_id,
          requested_at, income_source, is_customer, predicted_income,
          processed_at, sent_to_npl
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, false)
        ON CONFLICT (request_id)
        DO UPDATE SET
          predicted_income = EXCLUDED.predicted_income,
          processed_at = CURRENT_TIMESTAMP,
          sent_to_npl = false
        """,
        // Parameter setter
        new PredictionRecordJdbcBuilder(),
        // JDBC connection options
        JdbcExecutionOptions.builder()
          .withBatchSize(1000)
          .withBatchIntervalMs(200)
          .withMaxRetries(5)
          .build(),
        // JDBC connection options
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withUrl("jdbc:postgresql://localhost:5432/loan_db")
          .withDriverName("org.postgresql.Driver")
          .withUsername("docker")
          .withPassword("docker")
          .build()
      )
    )

    processedStream.print()
    // Execute job
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    incomePredictionRequestConsumer()
  }
}