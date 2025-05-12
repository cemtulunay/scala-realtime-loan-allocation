package loan

import org.apache.avro.generic.{GenericRecord}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.connector.jdbc.{JdbcStatementBuilder}
import target.StreamConsumer
import java.sql.PreparedStatement

object nonPerformingLoanService {

  /*********** event2 - Consumer ************/

  // Case class for npl prediction record
  case class PredictionResultRecord(
                                     requestId: String,
                                     applicationId: String,
                                     customerId: Int,
                                     prospectId: Int,
                                     requestedAt: Long,
                                     incomeSource: String,
                                     isCustomer: Boolean,
                                     predictedIncome: Double,
                                     processedAt: Long,
                                     sentToNpl: Boolean
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
      |    {"name": "requestedAt", "type": "long"},
      |    {"name": "incomeSource", "type": ["null","string"], "default": null},
      |    {"name": "isCustomer", "type": "boolean"},
      |    {"name": "predictedIncome", "type": "double"},
      |    {"name": "processedAt", "type": "long"},
      |    {"name": "sentToNpl", "type": "boolean"}
      |  ]
      |}
    """.stripMargin,
    consumerGroupId = "income_prediction_result_consumer",
    printToConsole = true
  ) {

    override protected def getSqlInsertStatement(): String = {
      """
        INSERT INTO income_predictions_result (
          request_id, application_id, customer_id, prospect_id,
          requested_at, income_source, is_customer, predicted_income,
          processed_at, sent_to_npl, ldes_processed_at, sent_to_ldes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, to_timestamp(? / 1000.0), true, CURRENT_TIMESTAMP, false)
        ON CONFLICT (request_id)
        DO UPDATE SET
          ldes_processed_at = CURRENT_TIMESTAMP,
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
          statement.setLong(5, record.requestedAt)
          statement.setString(6, record.incomeSource)
          statement.setBoolean(7, record.isCustomer)
          statement.setDouble(8, record.predictedIncome)
          statement.setLong(9, record.processedAt)
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
              requestedAt = record.get("requestedAt").toString.toLong,
              incomeSource = Option(record.get("incomeSource")).map(_.toString).orNull,
              isCustomer = Option(record.get("isCustomer")).exists(_.toString.toBoolean),
              predictedIncome = record.get("predictedIncome").toString.toDouble,
              processedAt = record.get("processedAt").toString.toLong,
              sentToNpl = Option(record.get("sentToNpl")).exists(_.toString.toBoolean)
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
    new IncomePredictionResultConsumer().execute()
  }

}
