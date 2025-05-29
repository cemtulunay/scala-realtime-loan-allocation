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
 * Case class representing loan decision analytics data
 */
case class LoanAnalytics(
                          timestamp: Long,
                          isApproved: Boolean,
                          loanAmount: Double,
                          riskScore: Double,
                          processingTime: Long
                        ) extends Serializable

/**
 * Case class for aggregated analytics
 */
case class AggregatedLoanAnalytics(
                                    windowStart: Long,
                                    windowEnd: Long,
                                    approvedCount: Long,
                                    rejectedCount: Long,
                                    approvalRate: Double,
                                    totalAmount: Double,
                                    avgAmount: Double,
                                    avgRiskScore: Double,
                                    highRiskCount: Long,
                                    avgProcTime: Double
                                  ) extends Serializable

/**
 * Aggregate function for loan analytics
 */
class LoanAnalyticsAggregateFunction extends AggregateFunction[LoanAnalytics, LoanAnalyticsAccumulator, AggregatedLoanAnalytics] {

  override def createAccumulator(): LoanAnalyticsAccumulator = LoanAnalyticsAccumulator()

  override def add(value: LoanAnalytics, accumulator: LoanAnalyticsAccumulator): LoanAnalyticsAccumulator = {
    accumulator.copy(
      count = accumulator.count + 1,
      approvedCount = accumulator.approvedCount + (if (value.isApproved) 1 else 0),
      rejectedCount = accumulator.rejectedCount + (if (!value.isApproved) 1 else 0),
      totalAmount = accumulator.totalAmount + value.loanAmount,
      totalRiskScore = accumulator.totalRiskScore + value.riskScore,
      highRiskCount = accumulator.highRiskCount + (if (value.riskScore > 0.7) 1 else 0),
      totalProcTime = accumulator.totalProcTime + value.processingTime,
      windowStart = if (accumulator.windowStart == 0) value.timestamp else math.min(accumulator.windowStart, value.timestamp),
      windowEnd = math.max(accumulator.windowEnd, value.timestamp)
    )
  }

  override def getResult(accumulator: LoanAnalyticsAccumulator): AggregatedLoanAnalytics = {
    val totalCount = accumulator.count
    AggregatedLoanAnalytics(
      windowStart = accumulator.windowStart,
      windowEnd = accumulator.windowEnd,
      approvedCount = accumulator.approvedCount,
      rejectedCount = accumulator.rejectedCount,
      approvalRate = if (totalCount > 0) accumulator.approvedCount.toDouble / totalCount else 0.0,
      totalAmount = accumulator.totalAmount,
      avgAmount = if (accumulator.approvedCount > 0) accumulator.totalAmount / accumulator.approvedCount else 0.0,
      avgRiskScore = if (totalCount > 0) accumulator.totalRiskScore / totalCount else 0.0,
      highRiskCount = accumulator.highRiskCount,
      avgProcTime = if (totalCount > 0) accumulator.totalProcTime / totalCount else 0.0
    )
  }

  override def merge(a: LoanAnalyticsAccumulator, b: LoanAnalyticsAccumulator): LoanAnalyticsAccumulator = {
    a.copy(
      count = a.count + b.count,
      approvedCount = a.approvedCount + b.approvedCount,
      rejectedCount = a.rejectedCount + b.rejectedCount,
      totalAmount = a.totalAmount + b.totalAmount,
      totalRiskScore = a.totalRiskScore + b.totalRiskScore,
      highRiskCount = a.highRiskCount + b.highRiskCount,
      totalProcTime = a.totalProcTime + b.totalProcTime,
      windowStart = if (a.windowStart == 0) b.windowStart else if (b.windowStart == 0) a.windowStart else math.min(a.windowStart, b.windowStart),
      windowEnd = math.max(a.windowEnd, b.windowEnd)
    )
  }
}

/**
 * Accumulator for loan analytics aggregation
 */
case class LoanAnalyticsAccumulator(
                                     count: Long = 0,
                                     approvedCount: Long = 0,
                                     rejectedCount: Long = 0,
                                     totalAmount: Double = 0.0,
                                     totalRiskScore: Double = 0.0,
                                     highRiskCount: Long = 0,
                                     totalProcTime: Long = 0,
                                     windowStart: Long = 0,
                                     windowEnd: Long = 0
                                   ) extends Serializable

/**
 * Simple InfluxDB sink for writing aggregated analytics
 */
class InfluxDBSink(
                    influxUrl: String = "http://localhost:8086",
                    database: String = "loan-analytics",
                    username: String = "admin",
                    password: String = "admin"
                  ) extends SinkFunction[AggregatedLoanAnalytics] {

  import java.net.http.{HttpClient, HttpRequest, HttpResponse}
  import java.net.URI

  @transient private var httpClient: HttpClient = _

  private val org = "loan-org"
  private val token = "loan-analytics-token"
  private val bucket = "loan-analytics"

  private def initializeClient(): Unit = {
    if (httpClient == null) {
      httpClient = HttpClient.newHttpClient()
      println(s"Initialized InfluxDB client for bucket: $bucket, org: $org")
    }
  }

  override def invoke(value: AggregatedLoanAnalytics, context: SinkFunction.Context): Unit = {
    initializeClient()

    try {
      val timestamp = value.windowEnd * 1000000
      val lineProtocol = s"loan_decisions," +
        s"window_type=tumbling " +
        s"approved_count=${value.approvedCount}i," +
        s"rejected_count=${value.rejectedCount}i," +
        s"approval_rate=${value.approvalRate}," +
        s"total_amount=${value.totalAmount}," +
        s"avg_amount=${value.avgAmount}," +
        s"avg_risk_score=${value.avgRiskScore}," +
        s"high_risk_count=${value.highRiskCount}i," +
        s"avg_proc_time=${value.avgProcTime}," +
        s"window_start=${value.windowStart}i," +
        s"window_end=${value.windowEnd}i " +
        s"$timestamp"

      val writeUrl = s"$influxUrl/api/v2/write?org=$org&bucket=$bucket&precision=ns"

      val request = HttpRequest.newBuilder()
        .uri(URI.create(writeUrl))
        .header("Authorization", s"Token $token")
        .header("Content-Type", "text/plain")
        .POST(HttpRequest.BodyPublishers.ofString(lineProtocol))
        .build()

      val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())

      if (response.statusCode() >= 200 && response.statusCode() < 300) {
        println(s"✅ Successfully wrote analytics to InfluxDB: approved=${value.approvedCount}, rejected=${value.rejectedCount}, rate=${(value.approvalRate * 100).round}%")
      } else {
        println(s"❌ InfluxDB write failed with status: ${response.statusCode()}, body: ${response.body()}")
        println(s"   URL: $writeUrl")
        println(s"   Token: $token")
        println(s"   Org: $org, Bucket: $bucket")

        val timeStr = java.time.Instant.ofEpochMilli(value.windowEnd)
        println(s"📊 Analytics Data ($timeStr):")
        println(s"   ✅ Approved: ${value.approvedCount}, ❌ Rejected: ${value.rejectedCount}")
        println(s"   📈 Approval Rate: ${(value.approvalRate * 100).round}%")
        println(s"   💰 Avg Amount: $${value.avgAmount.round}")
        println(s"   📝 Line Protocol: $lineProtocol")
      }

    } catch {
      case e: Exception =>
        println(s"❌ Error writing to InfluxDB: ${e.getMessage}")
        println(s"📊 Analytics Data: approved=${value.approvedCount}, rejected=${value.rejectedCount}")
    }
  }
}

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