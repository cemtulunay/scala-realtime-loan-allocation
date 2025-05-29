//package utils
//
//import org.apache.avro.generic.GenericRecord
//import org.apache.flink.api.common.functions.MapFunction
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import target.AnalyticalStreamConsumer
//import com.influxdb.client.write.Point
//import com.influxdb.client.domain.WritePrecision
//import org.json4s._
//import org.json4s.jackson.Serialization.write
//import java.time.Instant
//import scala.concurrent.ExecutionContext.Implicits.{global => globalEc}
//import scala.concurrent.duration.Duration
//import scala.concurrent.{Await, Future}
//
//object analyticalServiceV3 {
//
//  implicit val formats: DefaultFormats.type = DefaultFormats
//
//  /*******************************************************************************/
//  /**************** Event 6 - Disbursement Analytics (InfluxDB) ****************/
//  /*******************************************************************************/
//
//  case class DisbursementRecord(
//                                 requestId: String,
//                                 applicationId: String,
//                                 customerId: Int,
//                                 loanAmount: Double,
//                                 riskScore: Double,
//                                 processingTimeMs: Long,
//                                 eventTime: Long
//                               ) extends Serializable
//
//  class DisbursementAnalyticalConsumer extends AnalyticalStreamConsumer[DisbursementRecord](
//    topicName = "loan_decision_result",
//    schemaString =
//      """
//        |{
//        |  "type": "record",
//        |  "name": "LoanDecisionResult",
//        |  "namespace": "loan",
//        |  "fields": [
//        |    {"name": "requestId", "type": "string"},
//        |    {"name": "applicationId", "type": "string"},
//        |    {"name": "customerId", "type": "int"},
//        |    {"name": "prospectId", "type": "int"},
//        |    {"name": "incomeRequestedAt", "type": "long"},
//        |    {"name": "e1ProducedAt", "type": "long"},
//        |    {"name": "systemTime", "type": "long"},
//        |    {"name": "incomeSource", "type": ["null", "string"]},
//        |    {"name": "isCustomer", "type": "boolean"},
//        |    {"name": "kafkaSource", "type": "string"},
//        |    {"name": "predictedIncome", "type": "double"},
//        |    {"name": "e1ConsumedAt", "type": "long"},
//        |    {"name": "sentToNpl", "type": "boolean"},
//        |    {"name": "e2ProducedAt", "type": "long"},
//        |    {"name": "e2ConsumedAt", "type": "long"},
//        |    {"name": "nplRequestedAt", "type": "long"},
//        |    {"name": "e3ProducedAt", "type": "long"},
//        |    {"name": "e3ConsumedAt", "type": "long"},
//        |    {"name": "creditScore", "type": "long"},
//        |    {"name": "sentToLdes", "type": "boolean"},
//        |    {"name": "e4ProducedAt", "type": "long"},
//        |    {"name": "predictedNpl", "type": "double"},
//        |    {"name": "debtToIncomeRatio", "type": "double"},
//        |    {"name": "employmentIndustry", "type": "string"},
//        |    {"name": "employmentStability", "type": "double"},
//        |    {"name": "loanDecision", "type": "boolean"},
//        |    {"name": "loanAmount", "type": "double"},
//        |    {"name": "e5ProducedAt", "type": "long"}
//        |  ]
//        |}
//      """.stripMargin,
//    consumerGroupId = "disbursement_analytical_consumer",
//    measurement = "disbursement_events"
//  ) {
//
//    override protected def createRecordMapper(): MapFunction[GenericRecord, DisbursementRecord] = {
//      new MapFunction[GenericRecord, DisbursementRecord] {
//        override def map(record: GenericRecord): DisbursementRecord = {
//          try {
//            val loanDecision = Option(record.get("loanDecision")).exists(_.toString.toBoolean)
//
//            // Only process approved loans
//            if (loanDecision) {
//              val currentTime = System.currentTimeMillis()
//              val systemTime = record.get("systemTime").toString.toLong
//              val processingTime = currentTime - systemTime
//
//              // Calculate risk score
//              val predictedNpl = record.get("predictedNpl").toString.toDouble
//              val debtToIncomeRatio = record.get("debtToIncomeRatio").toString.toDouble
//              val creditScore = record.get("creditScore").toString.toLong
//              val riskScore = (predictedNpl * 0.4) + (debtToIncomeRatio * 0.3) + ((1000 - creditScore) / 1000.0 * 0.3)
//
//              DisbursementRecord(
//                requestId = record.get("requestId").toString,
//                applicationId = record.get("applicationId").toString,
//                customerId = record.get("customerId").toString.toInt,
//                loanAmount = record.get("loanAmount").toString.toDouble,
//                riskScore = riskScore,
//                processingTimeMs = processingTime,
//                eventTime = currentTime
//              )
//            } else {
//              null // Will be filtered out
//            }
//          } catch {
//            case e: Exception =>
//              println(s"Error processing disbursement record: ${e.getMessage}")
//              e.printStackTrace()
//              throw e
//          }
//        }
//      }
//    }
//
//    override protected def convertToInfluxPoint(record: DisbursementRecord): Point = {
//      AnalyticalStreamConsumer.addCommonTags(
//        Point.measurement("disbursement_events")
//          .time(Instant.ofEpochMilli(record.eventTime), WritePrecision.MS)
//          .addTag("loan_size", if (record.loanAmount > 50000) "LARGE" else if (record.loanAmount > 20000) "MEDIUM" else "SMALL")
//          .addTag("risk_category", if (record.riskScore > 0.7) "HIGH" else if (record.riskScore > 0.4) "MEDIUM" else "LOW")
//          .addField("loan_amount", record.loanAmount)
//          .addField("risk_score", record.riskScore)
//          .addField("processing_time_ms", record.processingTimeMs.toDouble),
//        record.customerId,
//        record.applicationId
//      )
//    }
//
//    override protected def createCustomDataStream(env: StreamExecutionEnvironment): Unit = {
//      val rawStream = createMainDataStream(env).filter(_ != null)
//
//      // Simple implementation - just write raw events to InfluxDB
//      rawStream.addSink(new InfluxDBSink())
//
//      // Simple aggregation without windowing - process each record individually
//      val analyticsStream = rawStream
//        .map(record => {
//          // Create analytics for each disbursement record
//          Map(
//            "measurement" -> "disbursement_analytics",
//            "total_disbursements" -> 1L,
//            "total_volume" -> record.loanAmount,
//            "avg_loan_amount" -> record.loanAmount,
//            "min_loan_amount" -> record.loanAmount,
//            "max_loan_amount" -> record.loanAmount,
//            "avg_risk_score" -> record.riskScore,
//            "customer_id" -> record.customerId,
//            "timestamp" -> record.eventTime
//          )
//        })
//
//      // Send individual analytics to InfluxDB
//      analyticsStream
//        .map(analytics => write(analytics))
//        .addSink(new DisbursementAnalyticsSink())
//    }
//  }
//
//  /*******************************************************************************/
//  /********************** Analytics Sink Implementations ***********************/
//  /*******************************************************************************/
//
//  class DisbursementAnalyticsSink extends org.apache.flink.streaming.api.functions.sink.SinkFunction[String] {
//    override def invoke(value: String, context: org.apache.flink.streaming.api.functions.sink.SinkFunction.Context): Unit = {
//      sendToInfluxDB(value, "disbursement_analytics")
//    }
//  }
//
//  private def sendToInfluxDB(data: String, measurement: String): Unit = {
//    try {
//      import com.influxdb.client.InfluxDBClientFactory
//      import com.influxdb.client.write.Point
//      import com.influxdb.client.domain.WritePrecision
//      import org.json4s.jackson.JsonMethods._
//
//      implicit val formats: DefaultFormats.type = DefaultFormats
//
//      val json = parse(data)
//      val timestamp = Instant.now()
//
//      val influxDB = InfluxDBClientFactory.create(
//        "http://localhost:8086",
//        "loan-analytics-token".toCharArray,
//        "loan-org",
//        "loan-analytics"
//      )
//
//      val point = Point.measurement(measurement)
//        .time(timestamp, WritePrecision.MS)
//
//      // Add all fields from JSON as InfluxDB fields
//      json.values.asInstanceOf[Map[String, Any]].foreach {
//        case (key, value: Number) => point.addField(key, value.doubleValue())
//        case (key, value: String) if key != "measurement" =>
//          point.addTag(key, value)
//        case _ => // Skip non-numeric fields and measurement
//      }
//
//      val writeApi = influxDB.getWriteApi
//      writeApi.writePoint(point)
//      writeApi.close()
//      influxDB.close()
//
//      println(s"Successfully wrote analytics data to InfluxDB measurement: $measurement")
//
//    } catch {
//      case e: Exception =>
//        println(s"Error sending analytics to InfluxDB: ${e.getMessage}")
//        e.printStackTrace()
//    }
//  }
//
//  /*******************************************************************************/
//  /******************************** EXECUTION ************************************/
//  /*******************************************************************************/
//
//  def main(args: Array[String]): Unit = {
//    val disbursementAnalyticsFuture = Future {
//      new DisbursementAnalyticalConsumer().execute()
//    }(globalEc)
//
//
//    try {
//      Await.result(disbursementAnalyticsFuture, Duration.Inf)
//    } catch {
//      case e: Exception =>
//        println(s"Error in analytics processes: ${e.getMessage}")
//        e.printStackTrace()
//        System.exit(1)
//    }
//  }
//}