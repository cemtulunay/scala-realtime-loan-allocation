package target

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import serializer.{GenericAvroDeserializer, GenericRecordKryoSerializer}

import java.util.Properties

/**
 * A generalized abstract class for Flink-based Kafka consumers that read data from a Kafka topic
 * and save it to a database. This class handles the common pipeline setup for consuming,
 * processing, and storing data.
 *
 * @param topicName Name of the Kafka topic to consume from
 * @param schemaString Avro schema for deserializing Kafka messages
 * @param bootstrapServers Kafka server addresses (comma-separated)
 * @param consumerGroupId Identifier for the consumer group
 * @param jdbcUrl Connection URL for the target database
 * @param jdbcUsername Database username
 * @param jdbcPassword Database password
 * @param withBatchSize Number of records to batch before writing to database
 * @param withBatchIntervalMs Maximum time (ms) to wait before writing a batch
 * @param withMaxRetries Number of retries for database operations
 * @param withDriverName JDBC driver class name
 * @param checkpointingIntervalMs Interval (ms) between state checkpoints
 * @param autoOffsetReset Strategy for offset when none exists (latest/earliest)
 * @param enableAutoCommit Whether to auto-commit offsets to Kafka
 * @tparam T The target domain object type (must be Serializable)
 */

// The generic abstract base class for Stream Consumers
abstract class StreamConsumer[T <: Serializable](
                                                  topicName: String,
                                                  schemaString: String,
                                                  bootstrapServers: String = "localhost:9092",
                                                  consumerGroupId: String,
                                                  jdbcUrl: String = "jdbc:postgresql://localhost:5432/loan_db",
                                                  jdbcUsername: String = "docker",
                                                  jdbcPassword: String = "docker",
                                                  withBatchSize: Int = 1000,
                                                  withBatchIntervalMs: Int = 200,
                                                  withMaxRetries: Int = 5,
                                                  withDriverName: String ="org.postgresql.Driver",
                                                  checkpointingIntervalMs: Int = 10000,
                                                  autoOffsetReset: String = "latest",
                                                  enableAutoCommit: String = "false",
                                                  printToConsole: Boolean = false
                                                ) {

  protected val schema: Schema = new Schema.Parser().parse(schemaString)

  // 1-) Configure Kafka properties
  protected def createKafkaProperties(): Properties = {
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", bootstrapServers)
    kafkaProps.setProperty("group.id", consumerGroupId)
    kafkaProps.setProperty("auto.offset.reset", autoOffsetReset)
    kafkaProps.setProperty("enable.auto.commit", enableAutoCommit)
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
    env.enableCheckpointing(checkpointingIntervalMs)

    // Register custom Kryo serializer
    env.getConfig.addDefaultKryoSerializer(
      classOf[GenericRecord],
      classOf[GenericRecordKryoSerializer]
    )

    // 6.2-) Create source stream
    val stream = env.addSource(createKafkaConsumer())

    // 6.3-) Process the records
    val processedStream = stream.map(createRecordMapper())

    if (printToConsole) {
      processedStream.print()
    }

    // 6.4-) Save to database
    processedStream.addSink(
      JdbcSink.sink[T](
        getSqlInsertStatement(),
        createJdbcStatementBuilder(),
        JdbcExecutionOptions.builder()
          .withBatchSize(withBatchSize)
          .withBatchIntervalMs(withBatchIntervalMs)
          .withMaxRetries(withMaxRetries)
          .build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withUrl(jdbcUrl)
          .withDriverName(withDriverName)
          .withUsername(jdbcUsername)
          .withPassword(jdbcPassword)
          .build()
      )
    )

    // Execute job
    env.execute(s"Stream Consumer for $topicName")
  }
}