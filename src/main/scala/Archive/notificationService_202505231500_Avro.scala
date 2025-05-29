//package utils
//
//import java.io.Serializable
//import org.apache.avro.generic.GenericRecord
//import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
//import org.apache.flink.streaming.api.functions.sink.SinkFunction
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
//import org.apache.flink.streaming.api.windowing.time.Time
//import target.AnalyticalStreamConsumer
//import com.influxdb.client.write.Point
//import com.influxdb.client.domain.WritePrecision
//import org.json4s._
//import org.json4s.jackson.Serialization
//import org.json4s.jackson.Serialization.write
//
//import java.time.Instant
//import scala.concurrent.ExecutionContext.Implicits.{global => globalEc}
//import scala.concurrent.duration.Duration
//import scala.concurrent.{Await, Future}
//import org.apache.flink.streaming.api.datastream.{DataStream => JavaDataStream}
//import org.apache.kafka.clients.admin.AdminClient
//import org.apache.kafka.clients.consumer.KafkaConsumer
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
//
//import java.util.Properties
//import scala.collection.JavaConverters.iterableAsScalaIterableConverter
//
//object notificationService {
//
//  implicit val formats: DefaultFormats.type = DefaultFormats
//
//  /** **************************************************************************** */
//  /** **************** Event 5 - Notification Analytics (InfluxDB) ************* */
//  /** **************************************************************************** */
//
//  case class NotificationRecord(
//                                 requestId: String,
//                                 applicationId: String,
//                                 customerId: Int,
//                                 loanDecision: Boolean,
//                                 loanAmount: Double,
//                                 riskScore: Double,
//                                 creditScore: Long,
//                                 processingTimeMs: Long,
//                                 eventTime: Long
//                               ) extends Serializable
//
//  // Accumulator for aggregation
//  case class NotificationAccumulator(
//                                      count: Long = 0,
//                                      approvedCount: Long = 0,
//                                      rejectedCount: Long = 0,
//                                      totalAmount: Double = 0.0,
//                                      riskScoreSum: Double = 0.0,
//                                      highRiskCount: Long = 0,
//                                      procTimeSum: Double = 0.0
//                                    )
//
//  // Output metrics after aggregation
//  case class NotificationAggregate(
//                                    windowStart: Long,
//                                    windowEnd: Long,
//                                    totalCount: Long,
//                                    approvedCount: Long,
//                                    rejectedCount: Long,
//                                    approvalRate: Double,
//                                    totalAmount: Double,
//                                    avgAmount: Double,
//                                    avgRiskScore: Double,
//                                    highRiskCount: Long,
//                                    avgProcessingTime: Double
//                                  )
//
//  class NotificationAnalyticalConsumer extends AnalyticalStreamConsumer[NotificationRecord](
//    topicName = "loan_decision_result_notification",
//    schemaString =
//      """
//        |{
//        |  "type": "record",
//        |  "name": "LoanDecisionResultNotification",
//        |  "namespace": "loan",
//        |        "fields": [
//        |          {"name": "requestId", "type": "string"},
//        |          {"name": "applicationId", "type": "string"},
//        |          {"name": "customerId", "type": "int"},
//        |          {"name": "incomeRequestedAt", "type": ["null", "long"], "default": null},
//        |          {"name": "systemTime", "type": "long"},
//        |          {"name": "isCustomer", "type": "boolean"},
//        |          {"name": "nplRequestedAt", "type": ["null", "long"], "default": null},
//        |          {"name": "creditScore", "type": "long"},
//        |          {"name": "predictedNpl", "type": "double"},
//        |          {"name": "debtToIncomeRatio", "type": "double"},
//        |          {"name": "loanDecision", "type": "boolean"},
//        |          {"name": "loanAmount", "type": "double"},
//        |          {"name": "e5ProducedAt", "type": ["null", "long"], "default": null}
//        |        ]
//        |}
//    """.stripMargin,
//    consumerGroupId = "notification_analytical_consumer",
//    measurement = "notification_events"
//  ) with Serializable {
//
//    //    override protected def createRecordMapper(): MapFunction[GenericRecord, NotificationRecord] = {
//    //      new MapFunction[GenericRecord, NotificationRecord] {
//    //        override def map(record: GenericRecord): NotificationRecord = {
//    //          System.out.println(s"CRITICAL_createRecordMapper: Attempting to map record: $record")
//    //
//    //          try {
//    //
//    //            record.getSchema.getFields.forEach(field => {
//    //              val fieldName = field.name()
//    //              val fieldValue = record.get(fieldName)
//    //              System.out.println(s"CRITICAL_createRecordMapper: Field $fieldName = $fieldValue (Type: ${fieldValue.getClass})")
//    //            })
//    //
//    //            val currentTime = System.currentTimeMillis()
//    ////            val systemTime = record.get("systemTime").toString.toLong
//    //            val systemTime = try {
//    //              record.get("systemTime").toString.toLong
//    //            } catch {
//    //              case e: Exception =>
//    //                System.out.println(s"CRITICAL ERROR: Failed to parse systemTime: ${e.getMessage}")
//    //                currentTime
//    //            }
//    //            val processingTime = currentTime - systemTime
//    //
//    //            // Calculate risk score
//    ////            val predictedNpl = record.get("predictedNpl").toString.toDouble
//    ////            val debtToIncomeRatio = record.get("debtToIncomeRatio").toString.toDouble
//    ////            val creditScore = record.get("creditScore").toString.toLong
//    ////            val riskScore = (predictedNpl * 0.4) + (debtToIncomeRatio * 0.3) + ((1000 - creditScore) / 1000.0 * 0.3)
//    //            val predictedNpl = try {
//    //              record.get("predictedNpl").toString.toDouble
//    //            } catch {
//    //              case e: Exception =>
//    //                System.out.println(s"CRITICAL ERROR: Failed to parse predictedNpl: ${e.getMessage}")
//    //                0.0
//    //            }
//    //
//    //            val debtToIncomeRatio = try {
//    //              record.get("debtToIncomeRatio").toString.toDouble
//    //            } catch {
//    //              case e: Exception =>
//    //                System.out.println(s"CRITICAL ERROR: Failed to parse debtToIncomeRatio: ${e.getMessage}")
//    //                0.0
//    //            }
//    //
//    //            val creditScore = try {
//    //              record.get("creditScore").toString.toLong
//    //            } catch {
//    //              case e: Exception =>
//    //                System.out.println(s"CRITICAL ERROR: Failed to parse creditScore: ${e.getMessage}")
//    //                0L
//    //            }
//    //
//    //            // Calculate risk score with error handling
//    //            val riskScore = try {
//    //              (predictedNpl * 0.4) + (debtToIncomeRatio * 0.3) + ((1000 - creditScore) / 1000.0 * 0.3)
//    //            } catch {
//    //              case e: Exception =>
//    //                System.out.println(s"CRITICAL ERROR: Failed to calculate risk score: ${e.getMessage}")
//    //                0.0
//    //            }
//    //
//    //
//    //            val mappedRecord = NotificationRecord(
//    //              requestId = record.get("requestId").toString,
//    //              applicationId = record.get("applicationId").toString,
//    //              customerId = record.get("customerId").toString.toInt,
//    //              loanDecision = Option(record.get("loanDecision")).exists(_.toString.toBoolean),
//    //              loanAmount = record.get("loanAmount").toString.toDouble,
//    //              riskScore = riskScore,
//    //              creditScore = creditScore,
//    //              processingTimeMs = processingTime,
//    //              eventTime = currentTime
//    //            )
//    //
//    //            System.out.println(s"CRITICAL: Successfully mapped record: $mappedRecord")
//    //            mappedRecord
//    //          } catch {
//    //            case e: Exception =>
//    //              println(s"Error processing notification record: ${e.getMessage}")
//    //              e.printStackTrace()
//    //              throw e
//    //          }
//    //        }
//    //      }
//    //    }
//
//    override protected def createRecordMapper(): MapFunction[Array[Byte], NotificationRecord] = {
//      new MapFunction[Array[Byte], NotificationRecord] {
//        override def map(bytes: Array[Byte]): NotificationRecord = {
//          System.out.println(s"CRITICAL_createRecordMapper: Attempting to map byte array of length: ${bytes.length}")
//
//          try {
//            analyzeBinaryFormat(bytes)
//
//            // Binary format parsing
//            var offset = 0
//
//            // Read first 'H' character and skip it
//            val firstChar = new String(bytes, 0, 1)
//            offset += 1
//
//            // Read requestId (assume it's a fixed-length UUID string)
//            val requestIdLength = 36 // Standard UUID length
//            val requestId = new String(bytes, offset, requestIdLength)
//            offset += requestIdLength
//
//            // Read second 'H' character and skip it
//            val secondChar = new String(bytes, offset, 1)
//            offset += 1
//
//            // Read applicationId (assume it's a fixed-length UUID string)
//            val applicationId = new String(bytes, offset, requestIdLength)
//            offset += requestIdLength
//
//            // The rest is binary data
//            // This part will require careful analysis of your specific format
//            // For now, let's create a debug record with the IDs we extracted
//
//            val currentTime = System.currentTimeMillis()
//
//            val mappedRecord = NotificationRecord(
//              requestId = requestId,
//              applicationId = applicationId,
//              customerId = 123, // Placeholder
//              loanDecision = true, // Placeholder
//              loanAmount = 10000.0, // Placeholder
//              riskScore = 0.5, // Placeholder
//              creditScore = 700L, // Placeholder
//              processingTimeMs = 0L,
//              eventTime = currentTime
//            )
//
//            System.out.println(s"CRITICAL: Extracted requestId=$requestId, applicationId=$applicationId")
//            mappedRecord
//          } catch {
//            case e: Exception =>
//              println(s"Error processing binary format: ${e.getMessage}")
//              e.printStackTrace()
//              throw e
//          }
//        }
//      }
//    }
//
//    override protected def convertToInfluxPoint(record: NotificationRecord): Point = {
//      val result: Point = AnalyticalStreamConsumer.addCommonTags(
//        Point.measurement("notification_events")
//          .time(Instant.ofEpochMilli(record.eventTime), WritePrecision.MS)
//          .addTag("loan_decision", record.loanDecision.toString)
//          .addTag("risk_category", if (record.riskScore > 0.7) "HIGH" else if (record.riskScore > 0.4) "MEDIUM" else "LOW")
//          .addField("loan_amount", record.loanAmount)
//          .addField("risk_score", record.riskScore)
//          .addField("credit_score", record.creditScore.toDouble)
//          .addField("processing_time_ms", record.processingTimeMs.toDouble),
//        record.customerId,
//        record.applicationId
//      )
//      println(result.toString)
//      result
//    }
//
//    override protected def createCustomDataStream(env: StreamExecutionEnvironment): Unit = {
//      // Import for implicit conversions
//      import org.apache.flink.streaming.api.scala._
//
//      // Create DataStream from raw events with proper conversion
//      println("Creating main data stream from Kafka...")
//      val javaStream = createMainDataStream(env)
//      val rawStream: DataStream[NotificationRecord] = new DataStream[NotificationRecord](javaStream)
//
//
//      // Very explicit print operations
//      println("Adding print operator with very explicit name...")
//      rawStream.map(record => {
//        println(s"DIRECT PRINTLN: Processing record: $record")
//        record
//      }).print("SUPER_EXPLICIT_PRINT")
//
//      // Add a print operator to see if data is flowing
//      println("Adding print operator to see raw data...")
//      rawStream.print("RAW_NOTIFICATION") // This will print to console
//
//      // Save raw events for flexibility (low-level data)
//      println("Adding InfluxDB sink for raw events...")
//      rawStream.addSink(new InfluxDBSink())
//
//      // Create 1-minute window aggregations with explicit typing
//      val aggregateFunction = new AggregateFunction[NotificationRecord, NotificationAccumulator, NotificationAggregate] with Serializable {
//
//        override def createAccumulator(): NotificationAccumulator = NotificationAccumulator()
//
//        override def add(record: NotificationRecord, acc: NotificationAccumulator): NotificationAccumulator = {
//          NotificationAccumulator(
//            count = acc.count + 1,
//            approvedCount = acc.approvedCount + (if (record.loanDecision) 1 else 0),
//            rejectedCount = acc.rejectedCount + (if (!record.loanDecision) 1 else 0),
//            totalAmount = acc.totalAmount + record.loanAmount,
//            riskScoreSum = acc.riskScoreSum + record.riskScore,
//            highRiskCount = acc.highRiskCount + (if (record.riskScore > 0.7) 1 else 0),
//            procTimeSum = acc.procTimeSum + record.processingTimeMs
//          )
//        }
//
//        override def getResult(acc: NotificationAccumulator): NotificationAggregate = {
//          val now = System.currentTimeMillis()
//          // val windowStart = now - 60000 // 1 minute ago
//          val windowStart = now - 2000 // 1 minute ago
//          val approvalRate = if (acc.count > 0) acc.approvedCount.toDouble / acc.count else 0.0
//          val avgAmount = if (acc.count > 0) acc.totalAmount / acc.count else 0.0
//          val avgRiskScore = if (acc.count > 0) acc.riskScoreSum / acc.count else 0.0
//          val avgProcTime = if (acc.count > 0) acc.procTimeSum / acc.count else 0.0
//
//          NotificationAggregate(
//            windowStart = windowStart,
//            windowEnd = now,
//            totalCount = acc.count,
//            approvedCount = acc.approvedCount,
//            rejectedCount = acc.rejectedCount,
//            approvalRate = approvalRate,
//            totalAmount = acc.totalAmount,
//            avgAmount = avgAmount,
//            avgRiskScore = avgRiskScore,
//            highRiskCount = acc.highRiskCount,
//            avgProcessingTime = avgProcTime
//          )
//        }
//
//        override def merge(acc1: NotificationAccumulator, acc2: NotificationAccumulator): NotificationAccumulator = {
//          NotificationAccumulator(
//            count = acc1.count + acc2.count,
//            approvedCount = acc1.approvedCount + acc2.approvedCount,
//            rejectedCount = acc1.rejectedCount + acc2.rejectedCount,
//            totalAmount = acc1.totalAmount + acc2.totalAmount,
//            riskScoreSum = acc1.riskScoreSum + acc2.riskScoreSum,
//            highRiskCount = acc1.highRiskCount + acc2.highRiskCount,
//            procTimeSum = acc1.procTimeSum + acc2.procTimeSum
//          )
//        }
//      }
//
//      // Define window and apply aggregation
//      println(s"Current time: ${System.currentTimeMillis()}, Window size: 2000ms")
//      val oneMinAggregates: DataStream[NotificationAggregate] = rawStream
//        //.windowAll(TumblingProcessingTimeWindows.of(Time.minutes(1)))
//        .map(record => {
//          println(s"Pre-window record: $record")
//          record
//        })
//        .windowAll(TumblingProcessingTimeWindows.of(Time.milliseconds(2000)))
//        //.trigger(new org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger())
//        .aggregate(aggregateFunction)
//
//      oneMinAggregates.print("DEBUG_CEM: oneMinAggregates created")
//      println(s"DEBUG_CEM_oneMinAggregates: {${oneMinAggregates.broadcast()}}")
//
//      // Convert to map and write to InfluxDB
//      oneMinAggregates
//        .map((agg: NotificationAggregate) => {
//          Map[String, Any](
//            "window_start" -> agg.windowStart,
//            "window_end" -> agg.windowEnd,
//            "total_notifications" -> agg.totalCount,
//            "approved_loans" -> agg.approvedCount,
//            "rejected_loans" -> agg.rejectedCount,
//            "approval_rate" -> agg.approvalRate,
//            "total_loan_amount" -> agg.totalAmount,
//            "avg_loan_amount" -> agg.avgAmount,
//            "avg_risk_score" -> agg.avgRiskScore,
//            "high_risk_loans" -> agg.highRiskCount,
//            "avg_processing_time" -> agg.avgProcessingTime,
//            "timestamp" -> System.currentTimeMillis(),
//            "window" -> "1min"
//          )
//        })
//        .map(metrics => write(metrics))
//        .addSink(new notificationService.AggregatedMetricsSink())
//
//      oneMinAggregates
//        .map(agg => s"Window[${agg.windowStart}-${agg.windowEnd}]: Count=${agg.totalCount}, " +
//          s"Approved=${agg.approvedCount}, Rejected=${agg.rejectedCount}, " +
//          s"ApprovalRate=${agg.approvalRate}, AvgAmount=${agg.avgAmount}, " +
//          s"AvgRiskScore=${agg.avgRiskScore}")
//        .print("DETAILED_AGGREGATION")
//
//      // Real-time high-risk alerts
//      rawStream
//        .filter(record => record.riskScore > 0.8 && record.loanDecision)
//        .map((record: NotificationRecord) => {
//          Map[String, Any](
//            "timestamp" -> record.eventTime,
//            "alert_type" -> "HIGH_RISK_APPROVED",
//            "severity" -> "HIGH",
//            "message" -> s"High-risk loan approved: ${record.requestId}",
//            "customer_id" -> record.customerId,
//            "risk_score" -> record.riskScore,
//            "loan_amount" -> record.loanAmount
//          )
//        })
//        .map(alert => write(alert))
//        .addSink(new notificationService.AlertSink())
//
//
//
//      rawStream.print("DEBUG_CEM: rawStream created")
//      println(rawStream.toString)
//    }
//  }
//
//  /*******************************************************************************/
//  /********************** Analytics Sink Implementations ***********************/
//  /*******************************************************************************/
//
//  class AggregatedMetricsSink extends SinkFunction[String] {
//    override def invoke(value: String, context: SinkFunction.Context): Unit = {
//      println("DEBUG_CEM_AggregatedMetricsSink: invoke called..")
//      sendToInfluxDB(value, "notification_metrics_aggregated")
//    }
//  }
//
//  class AlertSink extends SinkFunction[String] {
//    override def invoke(value: String, context: SinkFunction.Context): Unit = {
//      println("DEBUG_CEM_AlertSink: invoke called..")
//      sendToInfluxDB(value, "notification_alerts")
//    }
//  }
//
//  private def sendToInfluxDB(data: String, measurement: String): Unit = {
//    //    try {
//    //      println(s"Attempting to write data to InfluxDB: $data")
//    //
//    //      import com.influxdb.client.InfluxDBClientFactory
//    //      import com.influxdb.client.write.Point
//    //      import com.influxdb.client.domain.WritePrecision
//    //      import org.json4s.jackson.JsonMethods._
//    //
//    //      implicit val formats: DefaultFormats.type = DefaultFormats
//    //
//    //      val json = parse(data)
//    //      val timestamp = (json \ "timestamp").extractOpt[Long].getOrElse(System.currentTimeMillis())
//    //
//    //      val influxDB = InfluxDBClientFactory.create(
//    //        "http://localhost:8086",
//    //        "loan-analytics-token".toCharArray,
//    //        "loan-org",
//    //        "loan-analytics"
//    //      )
//    //
//    //      val point = Point.measurement(measurement)
//    //        .time(Instant.ofEpochMilli(timestamp), WritePrecision.MS)
//    //
//    //      println(s"Created point: ${point}")
//    //
//    //      // Add all fields from JSON as InfluxDB fields
//    //      json.values.asInstanceOf[Map[String, Any]].foreach {
//    //        case (key, value: Number) => point.addField(key, value.doubleValue())
//    //        case (key, value: String) if key != "measurement" && key != "timestamp" =>
//    //          point.addTag(key, value)
//    //        case _ => // Skip non-numeric fields, measurement and timestamp
//    //      }
//    //
//    //      val writeApi = influxDB.getWriteApi
//    //      writeApi.writePoint(point)
//    //      writeApi.close()
//    //      influxDB.close()
//    //
//    //      println(s"Successfully wrote analytics data to InfluxDB measurement: $measurement")
//    //
//    //    } catch {
//    //      case e: Exception =>
//    //        println(s"Error sending analytics to InfluxDB: ${e.getMessage}")
//    //        e.printStackTrace()
//    //    }
//    //    println(s"DEBUG: sendToInfluxDB called with data: $data") // Add this first line
//    //    println(s"DEBUG: Measurement: $measurement") // Add this second line
//    //
//    //    try {
//    //      println(s"Attempting to parse JSON data: $data")
//    //
//    //      import com.influxdb.client.InfluxDBClientFactory
//    //      import com.influxdb.client.write.Point
//    //      import com.influxdb.client.domain.WritePrecision
//    //      import org.json4s.jackson.JsonMethods._
//    //
//    //      implicit val formats: DefaultFormats.type = DefaultFormats
//    //
//    //      // Add explicit parsing debug
//    //      val jsonTry = try {
//    //        val parsedJson = parse(data)
//    //        println(s"DEBUG: Parsed JSON successfully: $parsedJson")
//    //        parsedJson
//    //      } catch {
//    //        case e: Exception =>
//    //          println(s"ERROR: Failed to parse JSON: ${e.getMessage}")
//    //          e.printStackTrace()
//    //          throw e
//    //      }
//    //
//    //      val timestamp = (jsonTry \ "timestamp").extractOpt[Long].getOrElse(System.currentTimeMillis())
//    //      println(s"DEBUG: Extracted timestamp: $timestamp")
//    //
//    //      val influxDB = InfluxDBClientFactory.create(
//    //        "http://localhost:8086",
//    //        "loan-analytics-token".toCharArray,
//    //        "loan-org",
//    //        "loan-analytics"
//    //      )
//    //
//    //      val point = Point.measurement(measurement)
//    //        .time(Instant.ofEpochMilli(timestamp), WritePrecision.MS)
//    //
//    //      println(s"Created point: ${point}")
//    //
//    //      // Add debug for JSON values
//    //      println(s"DEBUG: JSON values: ${jsonTry.values}")
//    //
//    //      // Add all fields from JSON as InfluxDB fields
//    //      jsonTry.values.asInstanceOf[Map[String, Any]].foreach { case (key, value) =>
//    //        println(s"DEBUG: Processing key: $key, value: $value, type: ${value.getClass}")
//    //        try {
//    //          value match {
//    //            case num: Number =>
//    //              println(s"Adding field: $key = ${num.doubleValue()}")
//    //              point.addField(key, num.doubleValue())
//    //            case str: String if key != "measurement" && key != "timestamp" =>
//    //              println(s"Adding tag: $key = $str")
//    //              point.addTag(key, str)
//    //            case _ =>
//    //              println(s"Skipping key: $key with value: $value")
//    //          }
//    //        } catch {
//    //          case e: Exception =>
//    //            println(s"ERROR processing key $key: ${e.getMessage}")
//    //            e.printStackTrace()
//    //        }
//    //      }
//    //
//    //      val writeApi = influxDB.getWriteApi
//    //      writeApi.writePoint(point)
//    //      writeApi.close()
//    //      influxDB.close()
//    //
//    //      println(s"Successfully wrote analytics data to InfluxDB measurement: $measurement")
//    //
//    //    } catch {
//    //      case e: Exception =>
//    //        println(s"CRITICAL ERROR sending analytics to InfluxDB: ${e.getMessage}")
//    //        println("Full stack trace:")
//    //        e.printStackTrace()
//    //    }
//    println(s"DEBUG: sendToInfluxDB called with data: $data")
//    println(s"DEBUG: Measurement: $measurement")
//
//    val maxRetries = 3
//    var retryCount = 0
//    var success = false
//
//    while (retryCount < maxRetries && !success) {
//      try {
//        println(s"Attempt ${retryCount + 1} to write to InfluxDB")
//
//        import com.influxdb.client.InfluxDBClientFactory
//        import com.influxdb.client.write.Point
//        import com.influxdb.client.domain.WritePrecision
//        import org.json4s.jackson.JsonMethods._
//
//        implicit val formats: DefaultFormats.type = DefaultFormats
//
//        val jsonTry = parse(data)
//        val timestamp = (jsonTry \ "timestamp").extractOpt[Long].getOrElse(System.currentTimeMillis())
//
//        val influxDB = InfluxDBClientFactory.create(
//          "http://localhost:8086",
//          "loan-analytics-token".toCharArray,
//          "loan-org",
//          "loan-analytics"
//        )
//
//        val point = Point.measurement(measurement)
//          .time(Instant.ofEpochMilli(timestamp), WritePrecision.MS)
//
//        jsonTry.values.asInstanceOf[Map[String, Any]].foreach { case (key, value) =>
//          try {
//            value match {
//              case num: Number =>
//                point.addField(key, num.doubleValue())
//                println(s"field added: $key")
//              case str: String if key != "measurement" && key != "timestamp" =>
//                point.addTag(key, str)
//                println(s"key processed: $key")
//              case _ => // Skip
//            }
//          } catch {
//            case e: Exception =>
//              println(s"ERROR processing key $key: ${e.getMessage}")
//          }
//        }
//
//        val writeApi = influxDB.getWriteApi
//        writeApi.writePoint(point)
//        writeApi.close()
//        influxDB.close()
//
//        println(s"Successfully wrote analytics data to InfluxDB measurement: $measurement")
//        success = true
//      } catch {
//        case e: Exception =>
//          println(s"ERROR sending analytics to InfluxDB (attempt ${retryCount + 1}): ${e.getMessage}")
//          e.printStackTrace()
//          retryCount += 1
//          if (retryCount < maxRetries) {
//            // Exponential backoff
//            Thread.sleep(1000 * Math.pow(2, retryCount).toLong)
//          }
//      }
//    }
//
//    if (!success) {
//      println(s"CRITICAL: Failed to write to InfluxDB after $maxRetries attempts")
//    }
//  }
//
//  /*******************************************************************************/
//  /******************************** EXECUTION ************************************/
//  /*******************************************************************************/
//
//  def produceTestMessage(): Unit = {
//    val props = new Properties()
//    props.put("bootstrap.servers", "localhost:9092")
//    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//
//    val producer = new KafkaProducer[String, String](props)
//
//    try {
//      val record = new ProducerRecord[String, String](
//        "loan_decision_result_notification",
//        "test-key",
//        """
//        {
//          "requestId": "test-request-1",
//          "applicationId": "test-app-1",
//          "customerId": 123,
//          "systemTime": 1623456789000,
//          "isCustomer": true,
//          "creditScore": 700,
//          "predictedNpl": 0.1,
//          "debtToIncomeRatio": 0.3,
//          "loanDecision": true,
//          "loanAmount": 50000.0
//        }
//        """
//      )
//
//      val metadata = producer.send(record).get()
//      println(s"Sent test message to topic: ${metadata.topic()}, partition: ${metadata.partition()}")
//    } finally {
//      producer.close()
//    }
//  }
//
//  def listKafkaTopics(): Unit = {
//    val props = new Properties()
//    props.put("bootstrap.servers", "localhost:9092")
//    props.put("group.id", "topic-list-consumer")
//    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//
//    val adminClient = AdminClient.create(props)
//
//    try {
//      val topicListing = adminClient.listTopics()
//      val topics = topicListing.names().get()
//
//      println("Available Kafka Topics:")
//      topics.forEach(topic => println(topic))
//    } finally {
//      adminClient.close()
//    }
//  }
//
//  def produceAvroTestMessage(): Unit = {
//    import org.apache.avro.generic.GenericData
//    import org.apache.avro.generic.GenericRecord
//    import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
//    import java.util.Properties
//    import org.apache.avro.Schema
//    import java.io.ByteArrayOutputStream
//    import org.apache.avro.io.EncoderFactory
//    import org.apache.avro.specific.SpecificDatumWriter
//
//    // Parse the schema
//    val schemaString = """
//  {
//    "type": "record",
//    "name": "LoanDecisionResultNotification",
//    "namespace": "loan",
//        "fields": [
//          {"name": "requestId", "type": "string"},
//          {"name": "applicationId", "type": "string"},
//          {"name": "customerId", "type": "int"},
//          {"name": "incomeRequestedAt", "type": ["null", "long"], "default": null},
//          {"name": "systemTime", "type": "long"},
//          {"name": "isCustomer", "type": "boolean"},
//          {"name": "nplRequestedAt", "type": ["null", "long"], "default": null},
//          {"name": "creditScore", "type": "long"},
//          {"name": "predictedNpl", "type": "double"},
//          {"name": "debtToIncomeRatio", "type": "double"},
//          {"name": "loanDecision", "type": "boolean"},
//          {"name": "loanAmount", "type": "double"},
//          {"name": "e5ProducedAt", "type": ["null", "long"], "default": null}
//        ]
//  }
//  """.stripMargin
//
//    val schema = new Schema.Parser().parse(schemaString)
//
//    // Create a generic record
//    val record = new GenericData.Record(schema)
//    record.put("requestId", "test-request-123")
//    record.put("applicationId", "test-app-456")
//    record.put("customerId", 789)
//    record.put("systemTime", System.currentTimeMillis())
//    record.put("isCustomer", true)
//    record.put("creditScore", 700L)
//    record.put("predictedNpl", 0.1)
//    record.put("debtToIncomeRatio", 0.3)
//    record.put("loanDecision", true)
//    record.put("loanAmount", 50000.0)
//
//    // Serialize the record
//    val writer = new SpecificDatumWriter[GenericRecord](schema)
//    val out = new ByteArrayOutputStream()
//    val encoder = EncoderFactory.get().binaryEncoder(out, null)
//    writer.write(record, encoder)
//    encoder.flush()
//    val serializedRecord = out.toByteArray()
//
//    // Produce the message
//    val props = new Properties()
//    props.put("bootstrap.servers", "localhost:9092")
//    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
//
//    val producer = new KafkaProducer[String, Array[Byte]](props)
//
//    try {
//      val producerRecord = new ProducerRecord[String, Array[Byte]](
//        "loan_decision_result_notification",
//        "test-key",
//        serializedRecord
//      )
//
//      val metadata = producer.send(producerRecord).get()
//      println(s"Sent Avro test message to topic: ${metadata.topic()}, partition: ${metadata.partition()}")
//    } finally {
//      producer.close()
//    }
//  }
//
//  def checkKafkaTopic(): Unit = {
//    import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig}
//    import java.util.Properties
//    //import scala.jdk.CollectionConverters._
//
//    val props = new Properties()
//    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
//    props.put(ConsumerConfig.GROUP_ID_CONFIG, "diagnostic-consumer")
//    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
//    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
//    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
//
//    val consumer = new KafkaConsumer[String, Array[Byte]](props)
//
//    try {
//      consumer.subscribe(java.util.Collections.singletonList("loan_decision_result_notification"))
//
//      val records = consumer.poll(java.time.Duration.ofSeconds(10))
//
//      println(s"DIAGNOSTIC: Number of records found: ${records.count()}")
//      records.records("loan_decision_result_notification").asScala.foreach { record =>
//        println(s"DIAGNOSTIC: Record - Key: ${record.key()}, Value length: ${record.value().length}")
//      }
//    } finally {
//      consumer.close()
//    }
//  }
//
//  def debugRawMessages(): Unit = {
//    println("\n===== EXAMINING RAW KAFKA MESSAGES =====")
//
//    val props = new Properties()
//    props.put("bootstrap.servers", "localhost:9092")
//    props.put("group.id", "raw-debug-" + System.currentTimeMillis())
//    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
//    props.put("auto.offset.reset", "earliest")
//
//    val consumer = new KafkaConsumer[String, Array[Byte]](props)
//
//    try {
//      consumer.subscribe(java.util.Collections.singletonList("loan_decision_result_notification"))
//
//      val records = consumer.poll(java.time.Duration.ofSeconds(5))
//      println(s"Found ${records.count()} records")
//
//      if (records.count() > 0) {
//        println("Examining first 3 records:")
//        var count = 0
//        records.asScala.take(3).foreach { record =>
//          count += 1
//          val bytes = record.value()
//          println(s"Record #$count: Key=${record.key()}, Size=${bytes.length} bytes")
//
//          // Try to identify the format
//          if (bytes.length > 0) {
//            // Check if it might be JSON
//            val firstFewBytes = new String(bytes.take(20))
//            println(s"First 20 bytes as string: '$firstFewBytes'")
//
//            if (firstFewBytes.trim.startsWith("{")) {
//              println("This appears to be a JSON message, not Avro binary!")
//              println("Full message as string:")
//              println(new String(bytes))
//            } else {
//              // Print first few bytes as hex
//              val hexBytes = bytes.take(10).map("%02X".format(_)).mkString(" ")
//              println(s"First 10 bytes as hex: $hexBytes")
//
//              // Check for Avro magic bytes (first byte of serialized avro is often 0)
//              if (bytes(0) == 0) {
//                println("This has characteristics of Avro binary data")
//              } else {
//                println("This doesn't appear to be typical Avro binary data")
//              }
//            }
//          }
//        }
//      }
//    } catch {
//      case e: Exception =>
//        println(s"Error debugging raw messages: ${e.getMessage}")
//        e.printStackTrace()
//    } finally {
//      consumer.close()
//    }
//
//    println("===== RAW MESSAGE EXAMINATION COMPLETE =====\n")
//  }
//
//
//  def testWithRawBytes(): Unit = {
//    println("\n===== TESTING WITH RAW BYTE DESERIALIZATION =====")
//
//    import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//    import org.apache.flink.api.common.serialization.SimpleStringSchema
//    import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    val props = new Properties()
//    props.setProperty("bootstrap.servers", "localhost:9092")
//    props.setProperty("group.id", "raw-flink-test-" + System.currentTimeMillis())
//    props.setProperty("auto.offset.reset", "earliest")
//
//    // Use string schema for simplicity
//    val consumer = new FlinkKafkaConsumer[String](
//      "loan_decision_result_notification",
//      new SimpleStringSchema(),
//      props
//    )
//
//    val stream = env.addSource(consumer)
//    stream.print("RAW-DATA")
//
//    try {
//      env.execute("Raw Kafka Test")
//    } catch {
//      case e: Exception =>
//        println(s"Error running raw test: ${e.getMessage}")
//        e.printStackTrace()
//    }
//
//    println("===== RAW TEST COMPLETE =====\n")
//  }
//
//  def analyzeBinaryFormat(bytes: Array[Byte]): Unit = {
//    println("Detailed binary format analysis:")
//    println(s"Total length: ${bytes.length} bytes")
//
//    // Print the whole array as hex for analysis
//    val hexDump = bytes.map("%02X".format(_)).grouped(16).map(_.mkString(" ")).mkString("\n")
//    println(s"Hex dump:\n$hexDump")
//
//    // Assuming the format is:
//    // 'H' + UUID + 'H' + UUID + binary data
//    val requestId = new String(bytes, 1, 36)
//    val applicationId = new String(bytes, 38, 36)
//
//    println(s"RequestId: $requestId")
//    println(s"ApplicationId: $applicationId")
//    println(s"Binary data starts at offset 74, length: ${bytes.length - 74} bytes")
//
//    // Try to interpret remaining bytes as different data types
//    val remainingBytes = bytes.drop(74)
//    println("Remaining bytes as:")
//
//    // As longs
//    if (remainingBytes.length >= 8) {
//      val longValue = java.nio.ByteBuffer.wrap(remainingBytes.take(8)).getLong()
//      println(s"Long (first 8 bytes): $longValue")
//    }
//
//    // As doubles
//    if (remainingBytes.length >= 8) {
//      val doubleValue = java.nio.ByteBuffer.wrap(remainingBytes.take(8)).getDouble()
//      println(s"Double (first 8 bytes): $doubleValue")
//    }
//
//    // As ints
//    if (remainingBytes.length >= 4) {
//      val intValue = java.nio.ByteBuffer.wrap(remainingBytes.take(4)).getInt()
//      println(s"Int (first 4 bytes): $intValue")
//    }
//  }
//
//  def main(args: Array[String]): Unit = {
//    println("Starting Notification Analytics Service...")
//
//    //checkKafkaTopic()
//
//    //produceAvroTestMessage()
//
//    // First, produce a test message
//    //produceTestMessage()
//
//    listKafkaTopics()
//
//    debugRawMessages()
//
//    //analyzeBinaryFormat(bytes)
//
//    //testWithRawBytes()
//
//    // First, test InfluxDB connection before starting the analytics process
//    testInfluxDBConnection()
//
//    println("Setting up detailed deserializer logging...")
//    System.setProperty("org.slf4j.simpleLogger.log.serializer.GenericAvroDeserializer", "debug")
//
//
//    // Create futures for analytics consumers
//    println("Initializing Notification Analytics Consumer...")
//    val notificationAnalyticsFuture = Future {
//      try {
//        new NotificationAnalyticalConsumer().execute()
//      } catch {
//        case e: Exception =>
//          println(s"ERROR: Analytics consumer execution failed: ${e.getMessage}")
//          e.printStackTrace()
//          throw e // Re-throw to be caught by the outer try/catch
//      }
//    }(globalEc)
//
//    try {
//      println("Waiting for analytics process to complete...")
//      Await.result(notificationAnalyticsFuture, Duration.Inf)
//    } catch {
//      case e: Exception =>
//        println("========== ANALYTICS SERVICE ERROR ==========")
//        println(s"Error in analytics processes: ${e.getMessage}")
//        println("Detailed stack trace:")
//        e.printStackTrace()
//
//        // Log the cause chain for nested exceptions
//        var cause = e.getCause
//        var level = 1
//        while (cause != null) {
//          println(s"Cause $level: ${cause.getClass.getName}: ${cause.getMessage}")
//          cause = cause.getCause
//          level += 1
//        }
//        println("===========================================")
//        System.exit(1)
//    }
//  }
//
//  // Add this helper method to test InfluxDB connection
//  private def testInfluxDBConnection(): Unit = {
//    println("Testing InfluxDB connection...")
//    try {
//      import com.influxdb.client.InfluxDBClientFactory
//
//      val influxDB = InfluxDBClientFactory.create(
//        "http://localhost:8086",
//        "loan-analytics-token".toCharArray,
//        "loan-org",
//        "loan-analytics"
//      )
//
//      // Check if we can connect
//      val health = influxDB.health()
//      println(s"InfluxDB connection test result: ${health.getStatus()}")
//      println(s"InfluxDB version: ${health.getVersion()}")
//
//      // Write a test point to verify write access
//      val writeApi = influxDB.getWriteApi
//      val point = Point.measurement("connection_test")
//        .addTag("test_type", "startup")
//        .addField("value", 1.0)
//        .time(Instant.now(), WritePrecision.MS)
//
//      writeApi.writePoint(point)
//      writeApi.close()
//
//      // Try to read data to verify read access
//      val queryApi = influxDB.getQueryApi()
//      val query = s"""from(bucket:"loan-analytics")
//                     |> range(start: -1m)
//                     |> filter(fn: (r) => r._measurement == "connection_test")
//                     |> limit(n:1)"""
//      val tables = queryApi.query(query)
//
//      if (tables.iterator().hasNext) {
//        println("Successfully read test data from InfluxDB")
//      } else {
//        println("WARNING: Could not read back test data - may indicate a lag or permission issue")
//      }
//
//      // Make sure to close the client
//      influxDB.close()
//      println("InfluxDB connection test completed successfully")
//
//    } catch {
//      case e: Exception =>
//        println("========== INFLUXDB CONNECTION ERROR ==========")
//        println(s"Failed to connect to InfluxDB: ${e.getMessage}")
//        e.printStackTrace()
//
//        println("\nPossible causes and solutions:")
//        println("1. InfluxDB is not running - check Docker container")
//        println("2. Incorrect connection URL - verify host/port")
//        println("3. Authentication failure - check token/credentials")
//        println("4. Network issue - check firewall/Docker network")
//        println("5. Bucket does not exist - verify bucket configuration")
//        println("==============================================")
//
//        // Don't exit, allow the program to continue and possibly retry
//        println("Continuing despite InfluxDB connection error...")
//    }
//  }
//
//}
