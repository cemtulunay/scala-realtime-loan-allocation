package target

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.functions.{MapFunction}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import serializer.{GenericAvroDeserializer, GenericRecordKryoSerializer}

// InfluxDB imports
import com.influxdb.client.{InfluxDBClient, InfluxDBClientFactory, WriteApi}
import com.influxdb.client.write.Point
import com.influxdb.client.domain.WritePrecision

import java.util.Properties
import java.time.Instant

/**
 * A generalized abstract class for Flink-based Kafka consumers that read data from a Kafka topic
 * and save it to InfluxDB for real-time analytics. This class handles the common pipeline setup
 * for consuming, processing, and storing time-series data.
 *
 * @param topicName Name of the Kafka topic to consume from
 * @param schemaString Avro schema for deserializing Kafka messages
 * @param bootstrapServers Kafka server addresses (comma-separated)
 * @param consumerGroupId Identifier for the consumer group
 * @param influxUrl InfluxDB connection URL
 * @param influxToken Authentication token for InfluxDB
 * @param influxOrg Organization name in InfluxDB
 * @param influxBucket Bucket name in InfluxDB
 * @param measurement InfluxDB measurement name (equivalent to table)
 * @param batchSize Number of points to batch before writing to InfluxDB
 * @param flushIntervalMs Maximum time (ms) to wait before flushing batch
 * @param checkpointingIntervalMs Interval (ms) between state checkpoints
 * @param autoOffsetReset Strategy for offset when none exists (latest/earliest)
 * @param enableAutoCommit Whether to auto-commit offsets to Kafka
 * @tparam T The target domain object type (must be Serializable)
 */
abstract class AnalyticalStreamConsumer[T <: Serializable](
                                                            topicName: String,
                                                            schemaString: String,
                                                            bootstrapServers: String = "localhost:9092",
                                                            consumerGroupId: String,
                                                            influxUrl: String = "http://localhost:8086",
                                                            influxToken: String = "loan-analytics-token",
                                                            influxOrg: String = "loan-org",
                                                            influxBucket: String = "loan-analytics",
                                                            measurement: String,
                                                            batchSize: Int = 1000,
                                                            flushIntervalMs: Int = 1000,
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

  // 3-) Map GenericRecord to domain object
  protected def createRecordMapper(): MapFunction[GenericRecord, T]

  // 4-) Convert domain object to InfluxDB Point (with tags and fields)
  protected def convertToInfluxPoint(record: T): Point

  // 5-) Create the main data stream - this is what was missing in your analytics service!
  protected def createMainDataStream(env: StreamExecutionEnvironment): DataStream[T] = {
    env.addSource(createKafkaConsumer())
      .map(createRecordMapper())
      .name(s"${topicName}_analytical_stream")
  }

  // 6-) Override this for custom stream processing (optional)
  protected def createCustomDataStream(env: StreamExecutionEnvironment): Unit = {
    // Default implementation just writes to InfluxDB
    val stream = createMainDataStream(env)

    if (printToConsole) {
      stream.print()
    }

    stream.addSink(new InfluxDBSink())
  }

  // 7-) InfluxDB Sink Implementation
  class InfluxDBSink extends RichSinkFunction[T] {
    private var influxClient: InfluxDBClient = _
    private var writeApi: WriteApi = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      influxClient = InfluxDBClientFactory.create(influxUrl, influxToken.toCharArray, influxOrg, influxBucket)
      writeApi = influxClient.getWriteApi
    }

    override def invoke(record: T, context: org.apache.flink.streaming.api.functions.sink.SinkFunction.Context): Unit = {
      try {
        val point = convertToInfluxPoint(record)
        writeApi.writePoint(point)
      } catch {
        case e: Exception =>
          println(s"Error writing to InfluxDB: ${e.getMessage}")
          e.printStackTrace()
          throw e
      }
    }

    override def close(): Unit = {
      if (writeApi != null) {
        writeApi.close()
      }
      if (influxClient != null) {
        influxClient.close()
      }
      super.close()
    }
  }

  // 8-) Execute the stream processing job
  def execute(): Unit = {
    // 8.1-) Setup Flink environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Enable checkpointing for reliability
    env.enableCheckpointing(checkpointingIntervalMs)

    // Register custom Kryo serializer
    env.getConfig.addDefaultKryoSerializer(
      classOf[GenericRecord],
      classOf[GenericRecordKryoSerializer]
    )

    // 8.2-) Create and execute custom data stream processing
    createCustomDataStream(env)

    // 8.3-) Execute job
    env.execute(s"Analytical Stream Consumer for $topicName")
  }
}

// Companion object with utility methods
object AnalyticalStreamConsumer {

  // Helper method to create common tags for InfluxDB points
  def addCommonTags(point: Point, customerId: Int, applicationId: String): Point = {
    point
      .addTag("customer_id", customerId.toString)
      .addTag("application_id", applicationId)
      .addTag("environment", "development") // or get from config
  }

  // Helper method to extract timestamp from various sources
  def extractTimestamp(systemTime: Option[Long], eventTime: Option[Long]): Instant = {
    val timestamp = eventTime.orElse(systemTime).getOrElse(System.currentTimeMillis())
    Instant.ofEpochMilli(timestamp)
  }

  // Helper method for time-based field calculations
  def calculateElapsedTime(startTime: Long, endTime: Long = System.currentTimeMillis()): Long = {
    endTime - startTime
  }
}