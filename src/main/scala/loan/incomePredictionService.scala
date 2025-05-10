package loan

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DatumReader, DecoderFactory, EncoderFactory}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.kafka.clients.producer.ProducerRecord

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

object incomePredictionService {



  /*********** event1 - Consumer ************/

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

  // The generic abstract base class for Stream Consumers
  abstract class StreamConsumer[T <: Serializable](
                                                    topicName: String,
                                                    schemaString: String,
                                                    bootstrapServers: String = "localhost:9092",
                                                    consumerGroupId: String,
                                                    jdbcUrl: String = "jdbc:postgresql://localhost:5432/loan_db",
                                                    jdbcUsername: String = "docker",
                                                    jdbcPassword: String = "docker"
                                                  ) {

    protected val schema: Schema = new Schema.Parser().parse(schemaString)

    // 1-) Configure Kafka properties
    protected def createKafkaProperties(): Properties = {
      val kafkaProps = new Properties()
      kafkaProps.setProperty("bootstrap.servers", bootstrapServers)
      kafkaProps.setProperty("group.id", consumerGroupId)
      kafkaProps.setProperty("auto.offset.reset", "latest")
      kafkaProps.setProperty("enable.auto.commit", "false")
      kafkaProps
    }

    // 2-) Create a Kafka consumer with generic Avro deserializer
    protected def createKafkaConsumer(): FlinkKafkaConsumer[GenericRecord] = {
      val consumer = new FlinkKafkaConsumer[GenericRecord](
        topicName,
        new GenericAvroDeserializer(schema),
        createKafkaProperties()
      )
      consumer.setStartFromEarliest()
      consumer
    }

    // 3-) Define the SQL insert statement
    protected def getSqlInsertStatement(): String

    // 4-) Create a JDBC statement builder
    protected def createJdbcStatementBuilder(): JdbcStatementBuilder[T]

    // 5-) Map GenericRecord to domain object - provides a critical translation layer between your streaming infrastructure and application-specific code
    //     Without createRecordMapper(), system would be forced to work directly with untyped GenericRecord objects throughout the pipeline. Would unable to leverage Scala's type system for safety
    protected def createRecordMapper(): MapFunction[GenericRecord, T]

    // 6-) Configure and execute the stream processing
    def execute(): Unit = {
      // 6.1-) Setup Flink environment
      val env = StreamExecutionEnvironment.getExecutionEnvironment

      // Enable checkpointing for reliability
      env.enableCheckpointing(10000)

      // Register custom Kryo serializer
      env.getConfig.addDefaultKryoSerializer(
        classOf[GenericRecord],
        classOf[GenericRecordKryoSerializer]
      )

      // 6.2-) Create source stream
      val stream = env.addSource(createKafkaConsumer())

      // 6.3-) Process the records
      val processedStream = stream.map(createRecordMapper())

      // 6.4-) Save to database
      processedStream.addSink(
        JdbcSink.sink[T](
          getSqlInsertStatement(),
          createJdbcStatementBuilder(),
          JdbcExecutionOptions.builder()
            .withBatchSize(1000)
            .withBatchIntervalMs(200)
            .withMaxRetries(5)
            .build(),
          new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl(jdbcUrl)
            .withDriverName("org.postgresql.Driver")
            .withUsername(jdbcUsername)
            .withPassword(jdbcPassword)
            .build()
        )
      )

      // Execute job
      env.execute(s"Stream Consumer for $topicName")
    }
  }

  // Case class for income prediction record
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

  // Implementation of the abstract class for income prediction
  class IncomePredictionConsumer extends StreamConsumer[PredictionRecord](
    topicName = "income_prediction_request",
    schemaString =
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
      """.stripMargin,
    consumerGroupId = "income_prediction_request_consumer"
  ) {

    override protected def getSqlInsertStatement(): String = {
      """
        |INSERT INTO income_predictions (
        |  request_id, application_id, customer_id, prospect_id,
        |  requested_at, income_source, is_customer, predicted_income,
        |  processed_at, sent_to_npl
        |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, false)
        |ON CONFLICT (request_id)
        |DO UPDATE SET
        |  predicted_income = EXCLUDED.predicted_income,
        |  processed_at = CURRENT_TIMESTAMP,
        |  sent_to_npl = false
      """.stripMargin
    }

    override protected def createJdbcStatementBuilder(): JdbcStatementBuilder[PredictionRecord] = {
      new JdbcStatementBuilder[PredictionRecord] {
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
    }

    override protected def createRecordMapper(): MapFunction[GenericRecord, PredictionRecord] = {
      new MapFunction[GenericRecord, PredictionRecord] {
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
    }
  }

  // Refactored method to use the generic consumer
  def incomePredictionRequestConsumer(): Unit = {
    // Create and execute the consumer
    val consumer = new IncomePredictionConsumer()
    consumer.execute()
  }

  /*********** event2 - Producer ************/



  // First, let's create a case class for the result records
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

  // Modified the source class to use SourceFunction instead of RichSourceFunction
  class PostgreSQLSource extends SourceFunction[PredictionResultRecord] with Serializable {
    @volatile private var connection: Connection = _
    @volatile private var preparedStatement: PreparedStatement = _
    @volatile private var isRunning: Boolean = true

    override def run(ctx: SourceFunction.SourceContext[PredictionResultRecord]): Unit = {
      try {
        // Initialize connection
        connection = DriverManager.getConnection(
          "jdbc:postgresql://localhost:5432/loan_db",
          "docker",
          "docker"
        )

        preparedStatement = connection.prepareStatement("""
        SELECT
          request_id, application_id, customer_id, prospect_id,
          requested_at, income_source, is_customer, predicted_income,
          processed_at, sent_to_npl
        FROM income_predictions
        WHERE sent_to_npl = false
        ORDER BY processed_at ASC
      """)

        while (isRunning) {
          val resultSet = preparedStatement.executeQuery()

          while (resultSet.next()) {
            val record = PredictionResultRecord(
              requestId = resultSet.getString("request_id"),
              applicationId = resultSet.getString("application_id"),
              customerId = resultSet.getInt("customer_id"),
              prospectId = resultSet.getInt("prospect_id"),
              requestedAt = resultSet.getLong("requested_at"),
              incomeSource = resultSet.getString("income_source"),
              isCustomer = resultSet.getBoolean("is_customer"),
              predictedIncome = resultSet.getDouble("predicted_income"),
              processedAt = resultSet.getTimestamp("processed_at").getTime,
              sentToNpl = resultSet.getBoolean("sent_to_npl")
            )

            ctx.collect(record)
            updateRecordAsProcessed(record.requestId)
          }

          resultSet.close()
          Thread.sleep(100)  // Poll every 0.1 second
        }
      } catch {
        case e: Exception =>
          println(s"Error in PostgreSQLSource: ${e.getMessage}")
          e.printStackTrace()
      } finally {
        cleanup()
      }
    }

    private def updateRecordAsProcessed(requestId: String): Unit = {
      var updateStmt: PreparedStatement = null
      try {
        updateStmt = connection.prepareStatement(
          "UPDATE income_predictions SET sent_to_npl = true WHERE request_id = ?"
        )
        updateStmt.setString(1, requestId)
        updateStmt.executeUpdate()
      } finally {
        if (updateStmt != null) {
          updateStmt.close()
        }
      }
    }

    private def cleanup(): Unit = {
      if (preparedStatement != null) {
        preparedStatement.close()
      }
      if (connection != null) {
        connection.close()
      }
    }

    override def cancel(): Unit = {
      isRunning = false
      cleanup()
    }
  }

  // Modified serializer class
  class PredictionResultSerializer extends KafkaSerializationSchema[PredictionResultRecord] {
    override def serialize(record: PredictionResultRecord, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
      val schema = new Schema.Parser().parse("""
      {
        "type": "record",
        "name": "IncomePredictionResult",
        "namespace": "loan",
        "fields": [
          {"name": "requestId", "type": "string"},
          {"name": "applicationId", "type": "string"},
          {"name": "customerId", "type": "int"},
          {"name": "prospectId", "type": "int"},
          {"name": "requestedAt", "type": "long"},
          {"name": "incomeSource", "type": ["null", "string"]},
          {"name": "isCustomer", "type": "boolean"},
          {"name": "predictedIncome", "type": "double"},
          {"name": "processedAt", "type": "long"},
          {"name": "sentToNpl", "type": "boolean"}
        ]
      }
    """)

      val avroRecord = new GenericData.Record(schema)
      avroRecord.put("requestId", record.requestId)
      avroRecord.put("applicationId", record.applicationId)
      avroRecord.put("customerId", record.customerId)
      avroRecord.put("prospectId", record.prospectId)
      avroRecord.put("requestedAt", record.requestedAt)
      avroRecord.put("incomeSource", record.incomeSource)
      avroRecord.put("isCustomer", record.isCustomer)
      avroRecord.put("predictedIncome", record.predictedIncome)
      avroRecord.put("processedAt", record.processedAt)  // Now using Long directly
      avroRecord.put("sentToNpl", record.sentToNpl)

      val writer = new GenericDatumWriter[GenericRecord](schema)
      val out = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(out, null)
      writer.write(avroRecord, encoder)
      encoder.flush()

      new ProducerRecord[Array[Byte], Array[Byte]](
        "income_prediction_result",
        record.requestId.getBytes(),
        out.toByteArray
      )
    }
  }

  def incomePredictionResultProducer(): Unit = {

    // 1-) Setup Environment and add postgreSql as a Source
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10000)

    val resultStream = env.addSource(new PostgreSQLSource())

    // 2-) Instantiate Kafka Producer with Avro serialization
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProps.setProperty("transaction.timeout.ms", "5000")
    kafkaProps.setProperty("retention.ms", "-1")  // infinite retention
    kafkaProps.setProperty("cleanup.policy", "delete")

    // 3-) Instantiate Kafka Producer with Avro serialization and pass Stream to Kafka Producer
    resultStream.addSink(new FlinkKafkaProducer[PredictionResultRecord](
      "income_prediction_result",
      new PredictionResultSerializer(),
      kafkaProps,
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE
    ))

    resultStream.print()
    env.execute("Income Prediction Result Producer")
  }

  def main(args: Array[String]): Unit = {
    // Create futures for both processes
    val consumerFuture = Future {
      incomePredictionRequestConsumer()
    }

    val producerFuture = Future {
      incomePredictionResultProducer()
    }

    // Combine futures and wait for both to complete
    val combinedFuture = Future.sequence(Seq(consumerFuture, producerFuture))

    try {
      // Wait indefinitely for both processes
      Await.result(combinedFuture, Duration.Inf)
    } catch {
      case e: Exception =>
        println(s"Error in one of the processes: ${e.getMessage}")
        e.printStackTrace()
        // You might want to gracefully shutdown both processes here
        System.exit(1)
    }
  }
}