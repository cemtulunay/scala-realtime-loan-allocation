package utils

import loan.utils.{randomCreditScore, skewedRandom}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.connector.jdbc.JdbcStatementBuilder
import target.StreamConsumer

import java.sql.PreparedStatement
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random

object nonPerformingLoanService {

  /*********** event2 - Consumer ************/

  // Case class for npl prediction record
  case class PredictionResultRecord(
                                     requestId: String,
                                     applicationId: String,
                                     customerId: Int,
                                     prospectId: Int,
                                     incomeRequestedAt: Long,
                                     e1ProducedAt: Long,
                                     incomeSource: String,
                                     sourceMicroService: String,
                                     isCustomer: Boolean,
                                     predictedIncome: Double,
                                     e1ConsumedAt: Long,
                                     sentToNpl: Boolean,
                                     e2ProducedAt: Long,
                                     creditScore: Int,
                                     predictedNpl: Double
                                   ) extends Serializable

  // Implementation of the abstract class for income prediction
  class IncomePredictionResultConsumer extends StreamConsumer[PredictionResultRecord](
    topicName = "income_prediction_result",
    schemaString =
    """
      |{
      |  "type": "record",
      |  "name": "IncomePredictionResult",
      |  "namespace": "loan",
      |  "fields": [
      |    {"name": "requestId", "type": "string"},
      |    {"name": "applicationId", "type": "string"},
      |    {"name": "customerId", "type": "int"},
      |    {"name": "prospectId", "type": "int"},
      |    {"name": "incomeRequestedAt", "type": "long"},
      |    {"name": "e1ProducedAt", "type": "long"},
      |    {"name": "incomeSource", "type": ["null", "string"]},
      |    {"name": "isCustomer", "type": "boolean"},
      |    {"name": "sourceMicroService", "type": "string"},
      |    {"name": "predictedIncome", "type": "double"},
      |    {"name": "e1ConsumedAt", "type": "long"},
      |    {"name": "sentToNpl", "type": "boolean"},
      |    {"name": "e2ProducedAt", "type": "long"}
      |  ]
      |}
    """.stripMargin,
    consumerGroupId = "income_prediction_result_consumer",
    printToConsole = false
  ) {

    override protected def getSqlInsertStatement(): String = {
      """
        INSERT INTO npl_prediction (
          request_id, application_id, customer_id, prospect_id,
          income_requested_at, e1_produced_at, income_source, is_customer,
          source_microservice, predicted_income, e1_consumed_at, sent_to_npl,
          e2_produced_at, e2_consumed_at, credit_score, sent_to_ldes, predicted_npl
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (extract(epoch from current_timestamp) * 1000)::bigint, ?, false, ?)
        ON CONFLICT (request_id)
        DO UPDATE SET
        credit_score = EXCLUDED.credit_score,
        predicted_npl = EXCLUDED.predicted_npl,
        sent_to_npl = false
        """.stripMargin
    }

    override protected def createJdbcStatementBuilder(): JdbcStatementBuilder[PredictionResultRecord] = {
      new JdbcStatementBuilder[PredictionResultRecord] {
        override def accept(statement: PreparedStatement, record: PredictionResultRecord): Unit = {
          statement.setString(1, record.requestId)
          statement.setString(2, record.applicationId)
          statement.setInt(3, record.customerId)
          statement.setInt(4, record.prospectId)
          statement.setLong(5, record.incomeRequestedAt)
          statement.setLong(6, record.e1ProducedAt)
          statement.setString(7, record.incomeSource)
          statement.setBoolean(8, record.isCustomer)
          statement.setString(9, record.sourceMicroService)
          statement.setDouble(10, record.predictedIncome)
          statement.setLong(11, record.e1ConsumedAt)
          statement.setBoolean(12, record.sentToNpl)
          statement.setLong(13, record.e2ProducedAt)
          statement.setDouble(14, record.creditScore)
          statement.setDouble(15, record.predictedNpl)
        }
      }
    }

    override protected def createRecordMapper(): MapFunction[GenericRecord, PredictionResultRecord] = {
      new MapFunction[GenericRecord, PredictionResultRecord] {
        override def map(record: GenericRecord): PredictionResultRecord = {
          try {
            val customerId = record.get("customerId").toString.toInt
            PredictionResultRecord(
              requestId = record.get("requestId").toString,
              applicationId = record.get("applicationId").toString,
              customerId = customerId,
              prospectId = record.get("prospectId").toString.toInt,
              incomeRequestedAt = record.get("incomeRequestedAt").toString.toLong,
              e1ProducedAt = record.get("e1ProducedAt").toString.toLong,
              incomeSource = Option(record.get("incomeSource")).map(_.toString).orNull,
              isCustomer = Option(record.get("isCustomer")).exists(_.toString.toBoolean),
              sourceMicroService = record.get("sourceMicroService").toString,
              predictedIncome = record.get("predictedIncome").toString.toDouble,
              e1ConsumedAt = record.get("e1ConsumedAt").toString.toLong,
              sentToNpl = Option(record.get("sentToNpl")).exists(_.toString.toBoolean),
              e2ProducedAt = record.get("e2ProducedAt").toString.toLong,
              creditScore = randomCreditScore(),
              predictedNpl = skewedRandom(),
            )
          } catch {
            case e: Exception =>
              println(s"Error processing record: ${e.getMessage}")
              e.printStackTrace()
              throw e
          }
        }
      }
    }
  }




  /*********** event3 - Consumer ************/

  // Case class for npl prediction record
  case class NplPredictionRequestRecord(
                                     requestId: String,
                                     applicationId: String,
                                     customerId: Int,
                                     prospectId: Int,
                                     requestedAt: Long,
                                     incomeSource: String,
                                     isCustomer: Boolean,
                                     predictedIncome: Double,
                                   ) extends Serializable

  // Implementation of the abstract class for income prediction
  class NplPredictionConsumer extends StreamConsumer[NplPredictionRequestRecord](
    topicName = "npl_prediction_request",
    schemaString =
      """
        |{
        |  "type": "record",
        |  "name": "NplPredictionRequest",
        |  "namespace": "loan",
        |  "fields": [
        |    {"name": "requestId", "type": "string"},
        |    {"name": "applicationId", "type": "string"},
        |    {"name": "customerId", "type": "int"},
        |    {"name": "prospectId", "type": "int"},
        |    {"name": "requestedAt", "type": "long"},
        |    {"name": "incomeSource", "type": ["null","string"], "default": null},
        |    {"name": "isCustomer", "type": "boolean"},
        |    {"name": "predictedIncome", "type": "double"}
        |  ]
        |}
    """.stripMargin,
    consumerGroupId = "npl_prediction_request_consumer",
    printToConsole = true
  ) {

    override protected def getSqlInsertStatement(): String = {
      """
        INSERT INTO npl_prediction (
          request_id, application_id, customer_id, prospect_id,
          requested_at, income_source, sourceMicroService, is_customer,
          predicted_income, processed_at, sent_to_npl, ldes_processed_at, sent_to_ldes
        ) VALUES (?, ?, ?, ?, ?, ?, 'npl_prediction_request', ?, ?, CURRENT_TIMESTAMP, true, CURRENT_TIMESTAMP, false)
        ON CONFLICT (request_id)
        DO UPDATE SET
          ldes_processed_at = CURRENT_TIMESTAMP,
          sent_to_npl = false
        """.stripMargin
    }

    override protected def createJdbcStatementBuilder(): JdbcStatementBuilder[NplPredictionRequestRecord] = {
      new JdbcStatementBuilder[NplPredictionRequestRecord] {
        override def accept(statement: PreparedStatement, record: NplPredictionRequestRecord): Unit = {
          statement.setString(1, record.requestId)
          statement.setString(2, record.applicationId)
          statement.setInt(3, record.customerId)
          statement.setInt(4, record.prospectId)
          statement.setLong(5, record.requestedAt)
          statement.setString(6, record.incomeSource)
          statement.setBoolean(7, record.isCustomer)
          statement.setDouble(8, record.predictedIncome)
        }
      }
    }

    override protected def createRecordMapper(): MapFunction[GenericRecord, NplPredictionRequestRecord] = {
      new MapFunction[GenericRecord, NplPredictionRequestRecord] {
        override def map(record: GenericRecord): NplPredictionRequestRecord = {
          try {
            val customerId = record.get("customerId").toString.toInt
            NplPredictionRequestRecord(
              requestId = record.get("requestId").toString,
              applicationId = record.get("applicationId").toString,
              customerId = customerId,
              prospectId = record.get("prospectId").toString.toInt,
              requestedAt = record.get("requestedAt").toString.toLong,
              incomeSource = Option(record.get("incomeSource")).map(_.toString).orNull,
              isCustomer = Option(record.get("isCustomer")).exists(_.toString.toBoolean),
              predictedIncome = record.get("predictedIncome").toString.toDouble
            )
          } catch {
            case e: Exception =>
              println(s"Error processing record: ${e.getMessage}")
              e.printStackTrace()
              throw e
          }
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    // Create futures for consumer and producer
      new IncomePredictionResultConsumer().execute()
    }


  /*
  def main(args: Array[String]): Unit = {
    // Create futures for consumer and producer
    val consumerFuture = Future {
      new IncomePredictionResultConsumer().execute()
    }

    val producerFuture = Future {
      // Directly create and execute the producer
      new NplPredictionConsumer().execute()
    }

    // Run both processes
    val combinedFuture = Future.sequence(Seq(consumerFuture, producerFuture))

    try {
      Await.result(combinedFuture, Duration.Inf)
    } catch {
      case e: Exception =>
        println(s"Error in one of the processes: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    }
  }
*/


}
