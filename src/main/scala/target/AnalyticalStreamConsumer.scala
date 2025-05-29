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
import utils.{AggregatedLoanAnalytics, InfluxDBSink, LoanAnalytics, LoanAnalyticsAggregateFunction}

import java.util.Properties


/**
 * A generalized abstract class for Flink-based Kafka consumers that read data from a Kafka topic
 * and perform analytical aggregations, then write the results to InfluxDB.
 */
abstract class LoanAnalyticalStreamConsumer[T <: Serializable](
                                                                topicName: String,
                                                                schemaString: String,
                                                                bootstrapServers: String = "localhost:9092",
                                                                consumerGroupId: String,
                                                                windowSizeSeconds: Int = 5,
                                                                influxUrl: String = "http://localhost:8086",
                                                                influxDatabase: String = "loan_analytics",
                                                                influxUsername: String = "admin",
                                                                influxPassword: String = "admin",
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

  protected def createRecordMapper(): MapFunction[GenericRecord, T]
  protected def convertToAnalytics(record: T): LoanAnalytics
  protected implicit def getTypeInformation: TypeInformation[T]

  def execute(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(checkpointingIntervalMs)
    env.getConfig.addDefaultKryoSerializer(
      classOf[GenericRecord],
      classOf[GenericRecordKryoSerializer]
    )

    implicit val loanAnalyticsTypeInfo: TypeInformation[LoanAnalytics] = createTypeInformation[LoanAnalytics]
    implicit val aggregatedTypeInfo: TypeInformation[AggregatedLoanAnalytics] = createTypeInformation[AggregatedLoanAnalytics]
    implicit val stringTypeInfo: TypeInformation[String] = createTypeInformation[String]

    val stream = env.addSource(createKafkaConsumer())
    val recordMapper = createRecordMapper()

    val processedStream = stream
      .map(recordMapper)
      .map((record: T) => convertToAnalytics(record))

    if (printToConsole) {
      processedStream.print()
    }

    val keyedStream = processedStream.keyBy((_: LoanAnalytics) => "all")
    val windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(windowSizeSeconds)))
    val aggregatedStream = windowedStream.aggregate(new LoanAnalyticsAggregateFunction())

    if (printToConsole) {
      aggregatedStream.print()
    }

    val influxSink = new InfluxDBSink(influxUrl, influxDatabase, influxUsername, influxPassword)
    aggregatedStream.addSink(influxSink)

    env.execute(s"Analytical Stream Consumer for $topicName")
  }
}