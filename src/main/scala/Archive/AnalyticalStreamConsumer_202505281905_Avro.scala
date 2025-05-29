//package target
//
//import org.apache.avro.Schema
//import org.apache.avro.generic.GenericRecord
//import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//import serializer.{GenericAvroDeserializer, GenericRecordKryoSerializer}
//import org.apache.flink.streaming.api.functions.sink.SinkFunction
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.api.scala.createTypeInformation
//
//import java.util.Properties
//import java.util.concurrent.TimeUnit
//import scala.collection.JavaConverters._
//import java.sql.{Connection, DriverManager, PreparedStatement}
//import java.time.Instant
//
///**
// * Case class representing loan decision analytics data
// */
//case class LoanAnalytics(
//                          timestamp: Long,
//                          isApproved: Boolean,
//                          loanAmount: Double,
//                          riskScore: Double,
//                          processingTime: Long
//                        ) extends Serializable
//
///**
// * Case class for aggregated analytics
// */
//case class AggregatedLoanAnalytics(
//                                    windowStart: Long,
//                                    windowEnd: Long,
//                                    approvedCount: Long,
//                                    rejectedCount: Long,
//                                    approvalRate: Double,
//                                    totalAmount: Double,
//                                    avgAmount: Double,
//                                    avgRiskScore: Double,
//                                    highRiskCount: Long,
//                                    avgProcTime: Double
//                                  ) extends Serializable
//
///**
// * Aggregate function for loan analytics
// */
//class LoanAnalyticsAggregateFunction extends AggregateFunction[LoanAnalytics, LoanAnalyticsAccumulator, AggregatedLoanAnalytics] {
//
//  override def createAccumulator(): LoanAnalyticsAccumulator = LoanAnalyticsAccumulator()
//
//  override def add(value: LoanAnalytics, accumulator: LoanAnalyticsAccumulator): LoanAnalyticsAccumulator = {
//    accumulator.copy(
//      count = accumulator.count + 1,
//      approvedCount = accumulator.approvedCount + (if (value.isApproved) 1 else 0),
//      rejectedCount = accumulator.rejectedCount + (if (!value.isApproved) 1 else 0),
//      totalAmount = accumulator.totalAmount + value.loanAmount,
//      totalRiskScore = accumulator.totalRiskScore + value.riskScore,
//      highRiskCount = accumulator.highRiskCount + (if (value.riskScore > 0.7) 1 else 0),
//      totalProcTime = accumulator.totalProcTime + value.processingTime,
//      windowStart = if (accumulator.windowStart == 0) value.timestamp else math.min(accumulator.windowStart, value.timestamp),
//      windowEnd = math.max(accumulator.windowEnd, value.timestamp)
//    )
//  }
//
//  override def getResult(accumulator: LoanAnalyticsAccumulator): AggregatedLoanAnalytics = {
//    val totalCount = accumulator.count
//    AggregatedLoanAnalytics(
//      windowStart = accumulator.windowStart,
//      windowEnd = accumulator.windowEnd,
//      approvedCount = accumulator.approvedCount,
//      rejectedCount = accumulator.rejectedCount,
//      approvalRate = if (totalCount > 0) accumulator.approvedCount.toDouble / totalCount else 0.0,
//      totalAmount = accumulator.totalAmount,
//      avgAmount = if (accumulator.approvedCount > 0) accumulator.totalAmount / accumulator.approvedCount else 0.0,
//      avgRiskScore = if (totalCount > 0) accumulator.totalRiskScore / totalCount else 0.0,
//      highRiskCount = accumulator.highRiskCount,
//      avgProcTime = if (totalCount > 0) accumulator.totalProcTime / totalCount else 0.0
//    )
//  }
//
//  override def merge(a: LoanAnalyticsAccumulator, b: LoanAnalyticsAccumulator): LoanAnalyticsAccumulator = {
//    a.copy(
//      count = a.count + b.count,
//      approvedCount = a.approvedCount + b.approvedCount,
//      rejectedCount = a.rejectedCount + b.rejectedCount,
//      totalAmount = a.totalAmount + b.totalAmount,
//      totalRiskScore = a.totalRiskScore + b.totalRiskScore,
//      highRiskCount = a.highRiskCount + b.highRiskCount,
//      totalProcTime = a.totalProcTime + b.totalProcTime,
//      windowStart = if (a.windowStart == 0) b.windowStart else if (b.windowStart == 0) a.windowStart else math.min(a.windowStart, b.windowStart),
//      windowEnd = math.max(a.windowEnd, b.windowEnd)
//    )
//  }
//}
//
///**
// * Accumulator for loan analytics aggregation
// */
//case class LoanAnalyticsAccumulator(
//                                     count: Long = 0,
//                                     approvedCount: Long = 0,
//                                     rejectedCount: Long = 0,
//                                     totalAmount: Double = 0.0,
//                                     totalRiskScore: Double = 0.0,
//                                     highRiskCount: Long = 0,
//                                     totalProcTime: Long = 0,
//                                     windowStart: Long = 0,
//                                     windowEnd: Long = 0
//                                   ) extends Serializable
//
///**
// * InfluxDB 2.x sink using HTTP API for writing aggregated analytics
// * This implementation uses HTTP calls to write data in InfluxDB line protocol format
// */
//class InfluxDBSink(
//                    influxUrl: String = "http://localhost:8086",
//                    database: String = "loan_analytics", // This will be the bucket name in InfluxDB 2.x
//                    username: String = "admin",
//                    password: String = "admin"
//                  ) extends SinkFunction[AggregatedLoanAnalytics] {
//
//  import java.net.http.{HttpClient, HttpRequest, HttpResponse}
//  import java.net.URI
//  import java.nio.charset.StandardCharsets
//
//  @transient private var httpClient: HttpClient = _
//
//  private def initializeClient(): Unit = {
//    if (httpClient == null) {
//      httpClient = HttpClient.newHttpClient()
//
//      // For InfluxDB 2.x, we don't need to create buckets via API
//      // Buckets should be created through the UI or CLI
//      println(s"Initialized InfluxDB client for bucket: $database")
//    }
//  }
//
//  override def invoke(value: AggregatedLoanAnalytics, context: SinkFunction.Context): Unit = {
//    initializeClient()
//
//    try {
//      // Convert to InfluxDB line protocol (same for both 1.x and 2.x)
//      val timestamp = value.windowEnd * 1000000 // Convert to nanoseconds
//      val lineProtocol = s"loan_decisions," +
//        s"window_type=tumbling " +
//        s"approved_count=${value.approvedCount}i," +
//        s"rejected_count=${value.rejectedCount}i," +
//        s"approval_rate=${value.approvalRate}," +
//        s"total_amount=${value.totalAmount}," +
//        s"avg_amount=${value.avgAmount}," +
//        s"avg_risk_score=${value.avgRiskScore}," +
//        s"high_risk_count=${value.highRiskCount}i," +
//        s"avg_proc_time=${value.avgProcTime}," +
//        s"window_start=${value.windowStart}i," +
//        s"window_end=${value.windowEnd}i " +
//        s"$timestamp"
//
//      // Try InfluxDB 2.x API first (with token auth)
//      val writeUrl2x = s"$influxUrl/api/v2/write?org=my-org&bucket=$database&precision=ns"
//
//      // For InfluxDB 2.x, you need a token. If you don't have one, you can get it from InfluxDB UI
//      // For now, let's try without auth first
//      val request2x = HttpRequest.newBuilder()
//        .uri(URI.create(writeUrl2x))
//        .header("Content-Type", "text/plain")
//        .POST(HttpRequest.BodyPublishers.ofString(lineProtocol))
//        .build()
//
//      val response2x = httpClient.send(request2x, HttpResponse.BodyHandlers.ofString())
//
//      if (response2x.statusCode() >= 200 && response2x.statusCode() < 300) {
//        println(s"✅ Successfully wrote analytics to InfluxDB 2.x: approved=${value.approvedCount}, rejected=${value.rejectedCount}, rate=${value.approvalRate}")
//      } else if (response2x.statusCode() == 404) {
//        // Try InfluxDB 1.x API as fallback
//        println("InfluxDB 2.x API not found, trying 1.x API...")
//        tryInfluxDB1x(lineProtocol, value)
//      } else {
//        println(s"❌ InfluxDB 2.x write failed with status: ${response2x.statusCode()}, body: ${response2x.body()}")
//        printFallbackData(value, lineProtocol)
//      }
//
//    } catch {
//      case e: Exception =>
//        println(s"❌ Error writing to InfluxDB: ${e.getMessage}")
//        e.printStackTrace()
//        printFallbackData(value, "")
//    }
//  }
//
//  private def tryInfluxDB1x(lineProtocol: String, value: AggregatedLoanAnalytics): Unit = {
//    try {
//      val writeUrl1x = s"$influxUrl/write?db=$database"
//
//      val request1x = HttpRequest.newBuilder()
//        .uri(URI.create(writeUrl1x))
//        .header("Content-Type", "text/plain")
//        .POST(HttpRequest.BodyPublishers.ofString(lineProtocol))
//        .build()
//
//      val response1x = httpClient.send(request1x, HttpResponse.BodyHandlers.ofString())
//
//      if (response1x.statusCode() >= 200 && response1x.statusCode() < 300) {
//        println(s"✅ Successfully wrote analytics to InfluxDB 1.x: approved=${value.approvedCount}, rejected=${value.rejectedCount}, rate=${value.approvalRate}")
//      } else {
//        println(s"❌ InfluxDB 1.x write failed with status: ${response1x.statusCode()}, body: ${response1x.body()}")
//        printFallbackData(value, lineProtocol)
//      }
//    } catch {
//      case e: Exception =>
//        println(s"❌ InfluxDB 1.x fallback failed: ${e.getMessage}")
//        printFallbackData(value, lineProtocol)
//    }
//  }
//
//  private def printFallbackData(value: AggregatedLoanAnalytics, lineProtocol: String): Unit = {
//    println(s"📊 Analytics Data (${java.time.Instant.ofEpochMilli(value.windowEnd)}):")
//    println(s"   Approved: ${value.approvedCount}, Rejected: ${value.rejectedCount}")
//    println(s"   Approval Rate: ${(value.approvalRate * 100).round}%")
//    println(s"   Avg Amount: ${value.avgAmount.round}, Avg Risk: ${(value.avgRiskScore * 100).round}%")
//    println(s"   High Risk Count: ${value.highRiskCount}, Avg Processing Time: ${(value.avgProcTime / 1000).round}s")
//    if (lineProtocol.nonEmpty) {
//      println(s"   Line Protocol: $lineProtocol")
//    }
//  }
//}
//
///**
// * A generalized abstract class for Flink-based Kafka consumers that read data from a Kafka topic
// * and perform analytical aggregations, then write the results to InfluxDB.
// *
// * @param topicName Name of the Kafka topic to consume from
// * @param schemaString Avro schema for deserializing Kafka messages
// * @param bootstrapServers Kafka server addresses (comma-separated)
// * @param consumerGroupId Identifier for the consumer group
// * @param windowSizeSeconds Window size in seconds for aggregations
// * @param influxUrl InfluxDB connection URL
// * @param influxDatabase InfluxDB database name
// * @param influxUsername InfluxDB username
// * @param influxPassword InfluxDB password
// * @param checkpointingIntervalMs Interval (ms) between state checkpoints
// * @param autoOffsetReset Strategy for offset when none exists (latest/earliest)
// * @param enableAutoCommit Whether to auto-commit offsets to Kafka
// * @tparam T The target domain object type (must be Serializable)
// */
//abstract class LoanAnalyticalStreamConsumer[T <: Serializable](
//                                                                topicName: String,
//                                                                schemaString: String,
//                                                                bootstrapServers: String = "localhost:9092",
//                                                                consumerGroupId: String,
//                                                                windowSizeSeconds: Int = 5,
//                                                                influxUrl: String = "http://localhost:8086",
//                                                                influxDatabase: String = "loan_analytics",
//                                                                influxUsername: String = "admin",
//                                                                influxPassword: String = "admin",
//                                                                checkpointingIntervalMs: Int = 10000,
//                                                                autoOffsetReset: String = "latest",
//                                                                enableAutoCommit: String = "false",
//                                                                printToConsole: Boolean = true
//                                                              ) {
//
//  protected val schema: Schema = new Schema.Parser().parse(schemaString)
//
//  // 1-) Configure Kafka properties
//  protected def createKafkaProperties(): Properties = {
//    val kafkaProps = new Properties()
//    kafkaProps.setProperty("bootstrap.servers", bootstrapServers)
//    kafkaProps.setProperty("group.id", consumerGroupId)
//    kafkaProps.setProperty("auto.offset.reset", autoOffsetReset)
//    kafkaProps.setProperty("enable.auto.commit", enableAutoCommit)
//    kafkaProps
//  }
//
//  // 2-) Create a Kafka consumer with generic Avro deserializer
//  protected def createKafkaConsumer(): FlinkKafkaConsumer[GenericRecord] = {
//    val consumer = new FlinkKafkaConsumer[GenericRecord](
//      topicName,
//      new GenericAvroDeserializer(schema),
//      createKafkaProperties()
//    )
//    consumer.setStartFromLatest()
//    consumer
//  }
//
//  // 3-) Map GenericRecord to domain object
//  protected def createRecordMapper(): MapFunction[GenericRecord, T]
//
//  // 4-) Convert domain object to LoanAnalytics for aggregation
//  protected def convertToAnalytics(record: T): LoanAnalytics
//
//  // 5-) Get type information for T
//  protected implicit def getTypeInformation: TypeInformation[T]
//
//  // 6-) Create a conversion mapper to avoid lambda capture
//  protected def createAnalyticsMapper(): MapFunction[T, LoanAnalytics] = {
//    new MapFunction[T, LoanAnalytics] with Serializable {
//      override def map(record: T): LoanAnalytics = convertToAnalytics(record)
//    }
//  }
//
//  // 5-) Configure and execute the stream processing
//  def execute(): Unit = {
//    // 5.1-) Setup Flink environment
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    // Enable checkpointing for reliability
//    env.enableCheckpointing(checkpointingIntervalMs)
//
//    // Register custom Kryo serializer
//    env.getConfig.addDefaultKryoSerializer(
//      classOf[GenericRecord],
//      classOf[GenericRecordKryoSerializer]
//    )
//
//    // Implicit type information
//    implicit val loanAnalyticsTypeInfo: TypeInformation[LoanAnalytics] = createTypeInformation[LoanAnalytics]
//    implicit val aggregatedTypeInfo: TypeInformation[AggregatedLoanAnalytics] = createTypeInformation[AggregatedLoanAnalytics]
//    implicit val stringTypeInfo: TypeInformation[String] = createTypeInformation[String]
//
//    // 5.2-) Create source stream
//    val stream = env.addSource(createKafkaConsumer())
//
//    // 5.3-) Process the records - use explicit mapper to avoid lambda capture
//    val recordMapper = createRecordMapper()
//    val analyticsMapper = createAnalyticsMapper()
//
//    val processedStream = stream
//      .map(recordMapper)
//      .map(analyticsMapper)
//
//    if (printToConsole) {
//      processedStream.print()
//    }
//
//    // 5.4-) Apply windowing and aggregation
//    val keyedStream = processedStream.keyBy((_: LoanAnalytics) => "all")
//    val windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(windowSizeSeconds)))
//    val aggregatedStream = windowedStream.aggregate(new LoanAnalyticsAggregateFunction())
//
//    if (printToConsole) {
//      aggregatedStream.print()
//    }
//
//    // 5.5-) Sink to InfluxDB
//    val influxSink = new InfluxDBSink(influxUrl, influxDatabase, influxUsername, influxPassword)
//    aggregatedStream.addSink(influxSink)
//
//    // Execute job
//    env.execute(s"Analytical Stream Consumer for $topicName")
//  }
//}
