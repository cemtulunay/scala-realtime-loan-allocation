package loan

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DatumReader, DecoderFactory, EncoderFactory}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{ResultTypeQueryable, TypeExtractor}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import loan.loanDecisionService.GenericAvroSerializer
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.kafka.clients.producer.ProducerRecord

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
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


  /*********** event2 - Producer ************/



  /**
   * A generic PostgreSQL source that can read any type of record from a PostgreSQL database.
   *
   * @param jdbcUrl The JDBC URL to connect to
   * @param username Database username
   * @param password Database password
   * @param query The SQL query to execute
   * @param recordMapper Function to map a ResultSet to a record of type T
   * @param updateQuery Optional query to update the processed record (e.g., mark as processed)
   * @param updateQueryParamSetter Optional function to set parameters for the update query
   * @param pollingInterval Interval in milliseconds between polling cycles
   * @tparam T The type of records to produce
   */
  class GenericPostgreSQLSource[T](
                                    jdbcUrl: String,
                                    username: String,
                                    password: String,
                                    query: String,
                                    recordMapper: ResultSet => T,
                                    updateQuery: Option[String] = None,
                                    updateQueryParamSetter: Option[(PreparedStatement, T) => Unit] = None,
                                    pollingInterval: Long = 100,
                                    private val typeInfo: TypeInformation[T]
                                  ) extends SourceFunction[T] with ResultTypeQueryable[T] with Serializable {

    @volatile private var connection: Connection = _
    @volatile private var preparedStatement: PreparedStatement = _
    @volatile private var isRunning: Boolean = true

    override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
      try {
        // Initialize connection
        connection = DriverManager.getConnection(
          jdbcUrl,
          username,
          password
        )

        preparedStatement = connection.prepareStatement(query)

        while (isRunning) {
          val resultSet = preparedStatement.executeQuery()

          while (resultSet.next()) {
            val record = recordMapper(resultSet)
            ctx.collect(record)

            // Update the record if an update query is provided
            (updateQuery, updateQueryParamSetter) match {
              case (Some(uQuery), Some(paramSetter)) => updateRecord(uQuery, record, paramSetter)
              case _ => // No update query or param setter
            }
          }

          resultSet.close()
          Thread.sleep(pollingInterval)
        }
      } catch {
        case e: Exception =>
          println(s"Error in GenericPostgreSQLSource: ${e.getMessage}")
          e.printStackTrace()
      } finally {
        cleanup()
      }
    }

    private def updateRecord(updateQuery: String, record: T, paramSetter: (PreparedStatement, T) => Unit): Unit = {
      var updateStmt: PreparedStatement = null
      try {
        updateStmt = connection.prepareStatement(updateQuery)
        paramSetter(updateStmt, record)
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

    override def getProducedType: TypeInformation[T] = typeInfo
  }

  /**
   * Abstract base class for PostgreSQL to Kafka producers using Flink.
   * This class provides a template for creating specific producers by extending it.
   *
   * @tparam T The type of records to produce
   */
  abstract class AbstractPostgreSQLToKafkaProducer[T] extends Serializable {

    // Abstract methods to be implemented by subclasses
    protected def getJobName: String
    protected def getJdbcUrl: String
    protected def getJdbcUsername: String
    protected def getJdbcPassword: String
    protected def getSelectQuery: String
    protected def getUpdateQuery: Option[String] = None
    protected def getUpdateQueryParamSetter: Option[(PreparedStatement, T) => Unit] = None
    protected def getRecordMapper: ResultSet => T
    protected def getPollingInterval: Long = 100
    protected def getKafkaTopic: String
    protected def getAvroSchema: String
    protected def getToGenericRecord: (T, GenericRecord) => Unit
    protected def getKeyExtractor: Option[T => Array[Byte]] = None
    protected def getKafkaProperties: Properties
    protected def getCheckpointInterval: Long = 10000
    protected def shouldPrintResults: Boolean = true
    protected def getTypeInformation: TypeInformation[T]

    /**
     * Main execution method that sets up and runs the Flink job.
     */
    def execute(): Unit = {
      // Setup Environment
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.enableCheckpointing(getCheckpointInterval)

      // Create the source
      val source = new GenericPostgreSQLSource[T](
        jdbcUrl = getJdbcUrl,
        username = getJdbcUsername,
        password = getJdbcPassword,
        query = getSelectQuery,
        recordMapper = getRecordMapper,
        updateQuery = getUpdateQuery,
        updateQueryParamSetter = getUpdateQueryParamSetter,
        pollingInterval = getPollingInterval,
        typeInfo = getTypeInformation
      )

      val resultStream = env.addSource(source)

      // Create the serializer
      val serializer = new GenericAvroSerializer[T](
        schemaString = getAvroSchema,
        topic = getKafkaTopic,
        toGenericRecord = getToGenericRecord,
        keyExtractor = getKeyExtractor
      )

      // Add Kafka sink
      resultStream.addSink(new FlinkKafkaProducer[T](
        getKafkaTopic,
        serializer,
        getKafkaProperties,
        FlinkKafkaProducer.Semantic.EXACTLY_ONCE
      ))

      // Optionally print the stream for debugging
      if (shouldPrintResults) {
        resultStream.print()
      }

      // Execute the job
      env.execute(getJobName)
    }
  }

  // Example usage with the original PredictionResultRecord

    // Define the case class for the original use case
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

    /**
     * Concrete implementation of AbstractPostgreSQLToKafkaProducer for income prediction results
     */
    class IncomePredictionResultProducer extends AbstractPostgreSQLToKafkaProducer[PredictionResultRecord] {

      override protected def getJobName: String = "Income Prediction Result Producer"

      override protected def getJdbcUrl: String = "jdbc:postgresql://localhost:5432/loan_db"

      override protected def getJdbcUsername: String = "docker"

      override protected def getJdbcPassword: String = "docker"

      override protected def getSelectQuery: String = """
      SELECT
        request_id, application_id, customer_id, prospect_id,
        requested_at, income_source, is_customer, predicted_income,
        processed_at, sent_to_npl
      FROM income_predictions
      WHERE sent_to_npl = false
      ORDER BY processed_at ASC
    """

      override protected def getUpdateQuery: Option[String] =
        Some("UPDATE income_predictions SET sent_to_npl = true WHERE request_id = ?")

      override protected def getUpdateQueryParamSetter: Option[(PreparedStatement, PredictionResultRecord) => Unit] =
        Some((stmt, record) => stmt.setString(1, record.requestId))

      override protected def getRecordMapper: ResultSet => PredictionResultRecord = rs =>
        PredictionResultRecord(
          requestId = rs.getString("request_id"),
          applicationId = rs.getString("application_id"),
          customerId = rs.getInt("customer_id"),
          prospectId = rs.getInt("prospect_id"),
          requestedAt = rs.getLong("requested_at"),
          incomeSource = rs.getString("income_source"),
          isCustomer = rs.getBoolean("is_customer"),
          predictedIncome = rs.getDouble("predicted_income"),
          processedAt = rs.getTimestamp("processed_at").getTime,
          sentToNpl = rs.getBoolean("sent_to_npl")
        )

      override protected def getKafkaTopic: String = "income_prediction_result"

      override protected def getAvroSchema: String = """
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
    """

      override protected def getToGenericRecord: (PredictionResultRecord, GenericRecord) => Unit = (record, avroRecord) => {
        avroRecord.put("requestId", record.requestId)
        avroRecord.put("applicationId", record.applicationId)
        avroRecord.put("customerId", record.customerId)
        avroRecord.put("prospectId", record.prospectId)
        avroRecord.put("requestedAt", record.requestedAt)
        avroRecord.put("incomeSource", record.incomeSource)
        avroRecord.put("isCustomer", record.isCustomer)
        avroRecord.put("predictedIncome", record.predictedIncome)
        avroRecord.put("processedAt", record.processedAt)
        avroRecord.put("sentToNpl", record.sentToNpl)
      }

      override protected def getKeyExtractor: Option[PredictionResultRecord => Array[Byte]] =
        Some(record => record.requestId.getBytes())

      override protected def getKafkaProperties: Properties = {
        val props = new Properties()
        props.setProperty("bootstrap.servers", "localhost:9092")
        props.setProperty("transaction.timeout.ms", "5000")
        props.setProperty("retention.ms", "-1")  // infinite retention
        props.setProperty("cleanup.policy", "delete")
        props
      }

      override protected def getTypeInformation: TypeInformation[PredictionResultRecord] = {
        TypeInformation.of(classOf[PredictionResultRecord])
      }

    }

  def main(args: Array[String]): Unit = {
    // Create futures for consumer and producer
    val consumerFuture = Future {
      new IncomePredictionConsumer().execute()
    }

    val producerFuture = Future {
      // Directly create and execute the producer
      new IncomePredictionResultProducer().execute()
    }

    // Run both processes
    val combinedFuture = Future.sequence(Seq(consumerFuture, producerFuture))

    try {
      Await.result(combinedFuture, Duration.Inf)
    } catch {
      case e: Exception =>
        println(s"Error in one of the processes: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    }
  }
}