package loan

import loan.incomePredictionService.GenericRecordKryoSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{DatumReader, DecoderFactory}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.io.ByteArrayInputStream
import java.sql.PreparedStatement
import java.util.Properties

object nonPerformingLoanService {

  // 7-) Define case class for postgreSql format
  case class PredictionResultRecord(
                                     requestId: String,
                                     applicationId: String,
                                     customerId: Int,
                                     prospectId: Int,
                                     requestedAt: Long,
                                     incomeSource: String,
                                     isCustomer: Boolean,
                                     predictedIncome: Double,
                                     processedAt: Long,
                                     sentToNpl: Boolean
                                   ) extends Serializable

  def incomePredictionResultConsumer(): Unit = {
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
        |  "name": "IncomePredictionResult",
        |  "namespace": "loan",
        |  "fields": [
        |    {"name": "requestId", "type": "string"},
        |    {"name": "applicationId", "type": "string"},
        |    {"name": "customerId", "type": "int"},
        |    {"name": "prospectId", "type": "int"},
        |    {"name": "requestedAt", "type": "long"},
        |    {"name": "incomeSource", "type": ["null","string"], "default": null},
        |    {"name": "isCustomer", "type": "boolean"},
        |    {"name": "predictedIncome", "type": "double"},
        |    {"name": "processedAt", "type": "long"},
        |    {"name": "sentToNpl", "type": "boolean"}
        |  ]
        |}
    """.stripMargin
    val schema = new Schema.Parser().parse(schemaString)

    // 3-) Configure Kafka properties
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProps.setProperty("group.id", "income_prediction_result_consumer")
    kafkaProps.setProperty("auto.offset.reset", "latest")
    kafkaProps.setProperty("enable.auto.commit", "false")

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
      "income_prediction_result",
      avroDeserializer,
      kafkaProps
    )

    kafkaConsumer.setStartFromLatest()

    // 6-) Add source to Flink environment and process
    val stream = env.addSource(kafkaConsumer)


    // 8-) Create a serializable function object for the mapping operation
    class RecordMapper extends MapFunction[GenericRecord, PredictionResultRecord] with Serializable {
      override def map(record: GenericRecord): PredictionResultRecord = {
        try {
          val customerId = record.get("customerId").toString.toInt
          PredictionResultRecord(
            requestId = record.get("requestId").toString,
            applicationId = record.get("applicationId").toString,
            customerId = customerId,
            prospectId = record.get("prospectId").toString.toInt,
            requestedAt = record.get("requestedAt").toString.toLong,
            incomeSource = Option(record.get("incomeSource")).map(_.toString).orNull,
            isCustomer = Option(record.get("isCustomer")).exists(_.toString.toBoolean),
            predictedIncome = record.get("predictedIncome").toString.toDouble,
            processedAt = record.get("processedAt").toString.toLong,
            sentToNpl = Option(record.get("sentToNpl")).exists(_.toString.toBoolean)
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
    class PredictionRecordJdbcBuilder extends JdbcStatementBuilder[PredictionResultRecord] with Serializable {
      override def accept(statement: PreparedStatement, record: PredictionResultRecord): Unit = {
        statement.setString(1, record.requestId)
        statement.setString(2, record.applicationId)
        statement.setInt(3, record.customerId)
        statement.setInt(4, record.prospectId)
        statement.setLong(5, record.requestedAt)
        statement.setString(6, record.incomeSource)
        statement.setBoolean(7, record.isCustomer)
        statement.setDouble(8, record.predictedIncome)
        statement.setLong(9, record.processedAt)
      }
    }

    // 11-) Save to PostgreSQL Serialized Data
    processedStream.addSink(
      JdbcSink.sink[PredictionResultRecord](
        // SQL statement
        """
        INSERT INTO income_predictions_result (
          request_id, application_id, customer_id, prospect_id,
          requested_at, income_source, is_customer, predicted_income,
          processed_at, sent_to_npl, ldes_processed_at, sent_to_ldes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, to_timestamp(? / 1000.0), true, CURRENT_TIMESTAMP, false)
        ON CONFLICT (request_id)
        DO UPDATE SET
          ldes_processed_at = CURRENT_TIMESTAMP,
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
    incomePredictionResultConsumer()
  }

}
