package target

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import serializer.{GenericAvroDeserializer, GenericRecordKryoSerializer}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala.createTypeInformation

import java.util.Properties

/**
 * A fully generic abstract class for Flink-based Kafka consumers that read data from a Kafka topic,
 * perform analytical aggregations, and write the results to a sink.
 *
 * @param topicName Name of the Kafka topic to consume from
 * @param schemaString Avro schema for deserializing Kafka messages
 * @param bootstrapServers Kafka server addresses (comma-separated)
 * @param consumerGroupId Identifier for the consumer group
 * @param windowSizeSeconds Size of the tumbling window in seconds
 * @param checkpointingIntervalMs Interval (ms) between state checkpoints
 * @param autoOffsetReset Strategy for offset when none exists (latest/earliest)
 * @param enableAutoCommit Whether to auto-commit offsets to Kafka
 * @param printToConsole Whether to print processed data to console for debugging
 * @tparam T The domain record type (must be Serializable)
 * @tparam A The analytics type that records are converted to (must be Serializable)
 * @tparam R The aggregated result type (must be Serializable)
 * @tparam ACC The accumulator type used for aggregation (must be Serializable)
 */
abstract class AnalyticalStreamConsumer[T <: Serializable, A <: Serializable, R <: Serializable, ACC <: Serializable](
                                                                                                                       topicName: String,
                                                                                                                       schemaString: String,
                                                                                                                       bootstrapServers: String = "localhost:9092",
                                                                                                                       consumerGroupId: String,
                                                                                                                       windowSizeSeconds: Int = 5,
                                                                                                                       checkpointingIntervalMs: Int = 10000,
                                                                                                                       autoOffsetReset: String = "latest",
                                                                                                                       enableAutoCommit: String = "false",
                                                                                                                       printToConsole: Boolean = true
                                                                                                                     ) {

  protected val schema: Schema = new Schema.Parser().parse(schemaString)

  protected def createKafkaProperties(): Properties = {
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", bootstrapServers)
    kafkaProps.setProperty("group.id", consumerGroupId)
    kafkaProps.setProperty("auto.offset.reset", autoOffsetReset)
    kafkaProps.setProperty("enable.auto.commit", enableAutoCommit)
    kafkaProps
  }

  protected def createKafkaConsumer(): FlinkKafkaConsumer[GenericRecord] = {
    val consumer = new FlinkKafkaConsumer[GenericRecord](
      topicName,
      new GenericAvroDeserializer(schema),
      createKafkaProperties()
    )
    consumer.setStartFromLatest()
    consumer
  }

  // Abstract methods that must be implemented by concrete classes
  protected def createRecordMapper(): MapFunction[GenericRecord, T]
  protected def createAnalyticsMapper(): MapFunction[T, A]
  protected def createAggregateFunction(): AggregateFunction[A, ACC, R]
  protected def createSink(): SinkFunction[R]

  // Type information methods
  protected implicit def getRecordTypeInformation: TypeInformation[T]
  protected implicit def getAnalyticsTypeInformation: TypeInformation[A]
  protected implicit def getResultTypeInformation: TypeInformation[R]

  def execute(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(checkpointingIntervalMs)
    env.getConfig.addDefaultKryoSerializer(
      classOf[GenericRecord],
      classOf[GenericRecordKryoSerializer]
    )

    implicit val stringTypeInfo: TypeInformation[String] = createTypeInformation[String]

    val stream = env.addSource(createKafkaConsumer())
    val recordMapper = createRecordMapper()
    val analyticsMapper = createAnalyticsMapper()

    val processedStream = stream
      .map(recordMapper)
      .map(analyticsMapper)

    if (printToConsole) {
      processedStream.print()
    }

    // Use asInstanceOf to work around Flink's type variance issues
    val keyedStream = processedStream.keyBy((_: A) => "all")
    val windowAssigner = TumblingProcessingTimeWindows.of(Time.seconds(windowSizeSeconds))
    val windowedStream = keyedStream.window(windowAssigner.asInstanceOf[org.apache.flink.streaming.api.windowing.assigners.WindowAssigner[A, org.apache.flink.streaming.api.windowing.windows.TimeWindow]])
    val aggregatedStream = windowedStream.aggregate(createAggregateFunction())

    if (printToConsole) {
      aggregatedStream.print()
    }

    aggregatedStream.addSink(createSink())

    env.execute(s"Analytical Stream Consumer for $topicName")
  }
}