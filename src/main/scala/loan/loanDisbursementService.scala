package utils

import loan.utils.{AggregatedLoanAnalytics, LoanAnalytics, LoanAnalyticsAccumulator, LoanAnalyticsAggregateFunction}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import target.AnalyticalStreamConsumer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object loanDisbursementService {


  /*******************************************************************************/
  /**************************** event6 - Consumer ********************************/
  /*******************************************************************************/


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

case class LoanDisbursementAnalyticsRecord (
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


// Now your concrete implementation using the generic base class
class LoanDecisionAnalyticsConsumer extends AnalyticalStreamConsumer[
  LoanDisbursementAnalyticsRecord,
  LoanAnalytics,
  AggregatedLoanAnalytics,
  LoanAnalyticsAccumulator
](
  topicName = "loan_decision_result_disbursement",
  schemaString =
    """
      |{
      |  "type": "record",
      |  "name": "LoanDecisionResultDisbursement",
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
    """.stripMargin,
  consumerGroupId = "loan_decision_analytics_consumer",
  windowSizeSeconds = 1
) {

  override protected def createRecordMapper(): MapFunction[GenericRecord, LoanDisbursementAnalyticsRecord] = {
    new MapFunction[GenericRecord, LoanDisbursementAnalyticsRecord] {
      override def map(record: GenericRecord): LoanDisbursementAnalyticsRecord = {
        try {
          println(s"Analytics Consumer - Processing record: ${record.toString}")

          LoanDisbursementAnalyticsRecord(
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
  }

  override protected def createAnalyticsMapper(): MapFunction[LoanDisbursementAnalyticsRecord, LoanAnalytics] = {
    new MapFunction[LoanDisbursementAnalyticsRecord, LoanAnalytics] {
      override def map(record: LoanDisbursementAnalyticsRecord): LoanAnalytics = {
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
  }

  // <IN, ACCUMULATOR, OUT>
  override protected def createAggregateFunction(): AggregateFunction[LoanAnalytics, LoanAnalyticsAccumulator, AggregatedLoanAnalytics] = {
    new LoanAnalyticsAggregateFunction()
  }

  override protected def createSink(): Option[SinkFunction[AggregatedLoanAnalytics]] = {
    Some(new InfluxDBSink("http://localhost:8086", "loan_analytics", "admin", "admin123"))
  }

  override protected implicit def getRecordTypeInformation: TypeInformation[LoanDisbursementAnalyticsRecord] =
    createTypeInformation[LoanDisbursementAnalyticsRecord]

  override protected implicit def getAnalyticsTypeInformation: TypeInformation[LoanAnalytics] =
    createTypeInformation[LoanAnalytics]

  override protected implicit def getResultTypeInformation: TypeInformation[AggregatedLoanAnalytics] =
    createTypeInformation[AggregatedLoanAnalytics]
}


  /*******************************************************************************/
  /******************************** EXECUTION ************************************/
  /*******************************************************************************/


  def main(args: Array[String]): Unit = {

    // Create futures for consumer and producer
    val consumerFuture = Future {
      new LoanDecisionAnalyticsConsumer().execute()
    }

    // Run both processes
    val combinedFuture = Future.sequence(Seq(consumerFuture))

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