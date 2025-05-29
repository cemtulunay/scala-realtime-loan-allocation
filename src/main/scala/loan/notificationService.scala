package utils

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import serializer.{GenericAvroDeserializer, GenericRecordKryoSerializer}
import target.{AggregatedLoanAnalytics, InfluxDBSink, LoanAnalytics, LoanAnalyticsAggregateFunction}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.util.{Base64, Properties}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

// Case class for loan decision notification record
case class LoanDecisionNotificationAnalyticsRecord(
                                                    requestId: String,
                                                    applicationId: String,
                                                    customerId: Int,
                                                    incomeRequestedAt: Long,
                                                    systemTime: Long,
                                                    isCustomer: Boolean,
                                                    nplRequestedAt: Long,
                                                    creditScore: Long,
                                                    predictedNpl: Double,
                                                    debtToIncomeRatio: Double,
                                                    loanDecision: Boolean,
                                                    loanAmount: Double,
                                                    e5ProducedAt: Long
                                                  ) extends Serializable

// Separate mapper class to avoid serialization issues
class LoanDecisionRecordMapper extends MapFunction[GenericRecord, LoanDecisionNotificationAnalyticsRecord] with Serializable {
  override def map(record: GenericRecord): LoanDecisionNotificationAnalyticsRecord = {
    try {
      println(s"Analytics Consumer - Processing record: ${record.toString}")

      LoanDecisionNotificationAnalyticsRecord(
        requestId = record.get("requestId").toString,
        applicationId = record.get("applicationId").toString,
        customerId = record.get("customerId").toString.toInt,
        incomeRequestedAt = Option(record.get("incomeRequestedAt")).map(_.toString.toLong).getOrElse(0L),
        systemTime = record.get("systemTime").toString.toLong,
        isCustomer = record.get("isCustomer").toString.toBoolean,
        nplRequestedAt = Option(record.get("nplRequestedAt")).map(_.toString.toLong).getOrElse(0L),
        creditScore = record.get("creditScore").toString.toLong,
        predictedNpl = record.get("predictedNpl").toString.toDouble,
        debtToIncomeRatio = record.get("debtToIncomeRatio").toString.toDouble,
        loanDecision = record.get("loanDecision").toString.toBoolean,
        loanAmount = record.get("loanAmount").toString.toDouble,
        e5ProducedAt = Option(record.get("e5ProducedAt")).map(_.toString.toLong).getOrElse(0L)
      )
    } catch {
      case e: Exception =>
        println(s"Analytics Consumer - Error processing record: ${e.getMessage}")
        e.printStackTrace()
        throw e
    }
  }
}

// Analytics mapper to avoid lambda capture issues
class LoanDecisionAnalyticsMapper extends MapFunction[LoanDecisionNotificationAnalyticsRecord, LoanAnalytics] with Serializable {
  override def map(record: LoanDecisionNotificationAnalyticsRecord): LoanAnalytics = {
    val currentTime = System.currentTimeMillis()

    // Calculate processing time
    val processingTime = if (record.isCustomer && record.nplRequestedAt > 0) {
      currentTime - record.nplRequestedAt
    } else if (record.incomeRequestedAt > 0) {
      currentTime - record.incomeRequestedAt
    } else {
      currentTime - record.systemTime
    }

    println(s"Converting to analytics - Request: ${record.requestId}, Decision: ${record.loanDecision}, Amount: ${record.loanAmount}, Risk: ${record.predictedNpl}, ProcTime: $processingTime")

    LoanAnalytics(
      timestamp = currentTime,
      isApproved = record.loanDecision,
      loanAmount = if (record.loanDecision) record.loanAmount else 0.0,
      riskScore = record.predictedNpl,
      processingTime = processingTime
    )
  }
}

// Standalone analytics consumer that doesn't extend abstract class
class LoanDecisionAnalyticsConsumer extends Serializable {

  val topicName = "loan_decision_result_notification"
  val consumerGroupId = "loan_decision_analytics_consumer"
  val windowSizeSeconds = 1
  val influxDatabase = "loan_analytics"
  val printToConsole = true
  val bootstrapServers = "localhost:9092"
  val checkpointingIntervalMs = 10000
  val autoOffsetReset = "latest"
  val enableAutoCommit = "false"
  val influxUrl = "http://localhost:8086"
  val influxUsername = "admin"
  val influxPassword = "admin123"

  val schemaString = notificationService.SCHEMA_STRING_LOAN_DECISION_NOTIFICATION
  @transient lazy val schema: Schema = new Schema.Parser().parse(schemaString)

  // Configure Kafka properties
  def createKafkaProperties(): Properties = {
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", bootstrapServers)
    kafkaProps.setProperty("group.id", consumerGroupId)
    kafkaProps.setProperty("auto.offset.reset", autoOffsetReset)
    kafkaProps.setProperty("enable.auto.commit", enableAutoCommit)
    kafkaProps
  }

  // Create a Kafka consumer with generic Avro deserializer
  def createKafkaConsumer(): FlinkKafkaConsumer[GenericRecord] = {
    val consumer = new FlinkKafkaConsumer[GenericRecord](
      topicName,
      new GenericAvroDeserializer(schema),
      createKafkaProperties()
    )
    consumer.setStartFromLatest()
    consumer
  }

  // Execute the stream processing
  def execute(): Unit = {
    // Setup Flink environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Enable checkpointing for reliability
    env.enableCheckpointing(checkpointingIntervalMs)

    // Register custom Kryo serializer
    env.getConfig.addDefaultKryoSerializer(
      classOf[GenericRecord],
      classOf[GenericRecordKryoSerializer]
    )

    // Implicit type information
    implicit val recordTypeInfo: TypeInformation[LoanDecisionNotificationAnalyticsRecord] = createTypeInformation[LoanDecisionNotificationAnalyticsRecord]
    implicit val loanAnalyticsTypeInfo: TypeInformation[LoanAnalytics] = createTypeInformation[LoanAnalytics]
    implicit val aggregatedTypeInfo: TypeInformation[AggregatedLoanAnalytics] = createTypeInformation[AggregatedLoanAnalytics]
    implicit val stringTypeInfo: TypeInformation[String] = createTypeInformation[String]

    // Create source stream
    val stream = env.addSource(createKafkaConsumer())

    // Process the records - use explicit mapper instances
    val recordMapper = new LoanDecisionRecordMapper()
    val analyticsMapper = new LoanDecisionAnalyticsMapper()

    val processedStream = stream
      .map(recordMapper)
      .map(analyticsMapper)

    if (printToConsole) {
      processedStream.print()
    }

    // Apply windowing and aggregation
    val keyedStream = processedStream.keyBy((_: LoanAnalytics) => "all")
    val windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(windowSizeSeconds)))
    val aggregatedStream = windowedStream.aggregate(new LoanAnalyticsAggregateFunction())

    if (printToConsole) {
      aggregatedStream.print()
    }

    // Sink to InfluxDB
    val influxSink = new InfluxDBSink(influxUrl, influxDatabase, influxUsername, influxPassword)
    aggregatedStream.addSink(influxSink)

    // Execute job
    env.execute(s"Analytical Stream Consumer for $topicName")
  }
}

class notificationService extends Serializable {

  /*******************************************************************************/
  /******************************** EXECUTION ************************************/
  /*******************************************************************************/

  // Execute method to run the analytics consumer
  def execute(): Unit = {
    println("Starting Loan Decision Analytics Service...")

    val analyticsConsumerFuture = Future {
      try {
        println("Initializing LoanDecisionAnalyticsConsumer...")
        val consumer = new LoanDecisionAnalyticsConsumer()
        println("Starting analytics consumer execution...")
        consumer.execute()
      } catch {
        case e: Exception =>
          println(s"Error in analytics consumer: ${e.getMessage}")
          e.printStackTrace()
          throw e
      }
    }

    try {
      println("Waiting for analytics consumer to complete...")
      Await.result(analyticsConsumerFuture, Duration.Inf)
    } catch {
      case e: Exception =>
        println(s"Error in notification service: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    }
  }
}

object notificationService {

  // Avro schema for loan decision notification
  val SCHEMA_STRING_LOAN_DECISION_NOTIFICATION =
    """
      |{
      |  "type": "record",
      |  "name": "LoanDecisionResultNotification",
      |  "namespace": "loan",
      |  "fields": [
      |    {"name": "requestId", "type": "string"},
      |    {"name": "applicationId", "type": "string"},
      |    {"name": "customerId", "type": "int"},
      |    {"name": "incomeRequestedAt", "type": ["null", "long"], "default": null},
      |    {"name": "systemTime", "type": "long"},
      |    {"name": "isCustomer", "type": "boolean"},
      |    {"name": "nplRequestedAt", "type": ["null", "long"], "default": null},
      |    {"name": "creditScore", "type": "long"},
      |    {"name": "predictedNpl", "type": "double"},
      |    {"name": "debtToIncomeRatio", "type": "double"},
      |    {"name": "loanDecision", "type": "boolean"},
      |    {"name": "loanAmount", "type": "double"},
      |    {"name": "e5ProducedAt", "type": ["null", "long"], "default": null}
      |  ]
      |}
    """.stripMargin

  // Main method to run the analytics service
  def main(args: Array[String]): Unit = {
    val service = new notificationService()
    service.execute()
  }
}