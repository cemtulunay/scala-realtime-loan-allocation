package loan

import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.connector.jdbc.JdbcStatementBuilder
import source.{AbstractPostgreSQLToKafkaProducer}
import target.StreamConsumer

import java.sql.{PreparedStatement, ResultSet}
import java.util.Properties
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

object incomePredictionService {

  /*********** event1 - Consumer ************/

  // Case class for income prediction record
  case class PredictionRecord(
                               requestId: String,
                               applicationId: String,
                               customerId: Int,
                               prospectId: Int,
                               requestedAt: Long,
                               incomeSource: String,
                               isCustomer: Boolean,
                               predictedIncome: Double
                             ) extends Serializable

  // Implementation of the abstract class for income prediction
  class IncomePredictionConsumer extends StreamConsumer[PredictionRecord](
    topicName = "income_prediction_request",
    schemaString =
      """
        |{
        |  "type": "record",
        |  "name": "IncomePredictionRequest",
        |  "namespace": "loan",
        |  "fields": [
        |    {"name": "requestId", "type": ["string"]},
        |    {"name": "applicationId", "type": ["string"]},
        |    {"name": "customerId", "type": ["int"]},
        |    {"name": "prospectId", "type": ["int"]},
        |    {"name": "requestedAt", "type": ["long"]},
        |    {"name": "incomeSource", "type": ["null","string"], "default": null},
        |    {"name": "isCustomer", "type": ["null","boolean"], "default": null}
        |  ]
        |}
      """.stripMargin,
    consumerGroupId = "income_prediction_request_consumer"
  ) {

    override protected def getSqlInsertStatement(): String = {
      """
        |INSERT INTO income_predictions (
        |  request_id, application_id, customer_id, prospect_id,
        |  requested_at, income_source, is_customer, predicted_income,
        |  processed_at, sent_to_npl
        |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, false)
        |ON CONFLICT (request_id)
        |DO UPDATE SET
        |  predicted_income = EXCLUDED.predicted_income,
        |  processed_at = CURRENT_TIMESTAMP,
        |  sent_to_npl = false
      """.stripMargin
    }

    override protected def createJdbcStatementBuilder(): JdbcStatementBuilder[PredictionRecord] = {
      new JdbcStatementBuilder[PredictionRecord] {
        override def accept(statement: PreparedStatement, record: PredictionRecord): Unit = {
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

    override protected def createRecordMapper(): MapFunction[GenericRecord, PredictionRecord] = {
      new MapFunction[GenericRecord, PredictionRecord] {
        override def map(record: GenericRecord): PredictionRecord = {
          try {
            val customerId = record.get("customerId").toString.toInt
            PredictionRecord(
              requestId = record.get("requestId").toString,
              applicationId = record.get("applicationId").toString,
              customerId = customerId,
              prospectId = record.get("prospectId").toString.toInt,
              requestedAt = record.get("requestedAt").toString.toLong,
              incomeSource = Option(record.get("incomeSource")).map(_.toString).orNull,
              isCustomer = Option(record.get("isCustomer")).exists(_.toString.toBoolean),
              predictedIncome = 50000.0 + (customerId % 10) * 5000.0
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


  /*********** event2 - Producer ************/

    // Define the case class for the original use case
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

    /**
     * Concrete implementation of AbstractPostgreSQLToKafkaProducer for income prediction results
     */
    class IncomePredictionResultProducer extends AbstractPostgreSQLToKafkaProducer[PredictionResultRecord] {

      override protected def getJobName: String = "Income Prediction Result Producer"

      override protected def getJdbcUrl: String = "jdbc:postgresql://localhost:5432/loan_db"

      override protected def getJdbcUsername: String = "docker"

      override protected def getJdbcPassword: String = "docker"

      override protected def getSelectQuery: String = """
      SELECT
        request_id, application_id, customer_id, prospect_id,
        requested_at, income_source, is_customer, predicted_income,
        processed_at, sent_to_npl
      FROM income_predictions
      WHERE sent_to_npl = false
      ORDER BY processed_at ASC
    """

      override protected def getUpdateQuery: Option[String] =
        Some("UPDATE income_predictions SET sent_to_npl = true WHERE request_id = ?")

      override protected def getUpdateQueryParamSetter: Option[(PreparedStatement, PredictionResultRecord) => Unit] =
        Some((stmt, record) => stmt.setString(1, record.requestId))

      override protected def getRecordMapper: ResultSet => PredictionResultRecord = rs =>
        PredictionResultRecord(
          requestId = rs.getString("request_id"),
          applicationId = rs.getString("application_id"),
          customerId = rs.getInt("customer_id"),
          prospectId = rs.getInt("prospect_id"),
          requestedAt = rs.getLong("requested_at"),
          incomeSource = rs.getString("income_source"),
          isCustomer = rs.getBoolean("is_customer"),
          predictedIncome = rs.getDouble("predicted_income"),
          processedAt = rs.getTimestamp("processed_at").getTime,
          sentToNpl = rs.getBoolean("sent_to_npl")
        )

      override protected def getKafkaTopic: String = "income_prediction_result"

      override protected def getAvroSchema: String = """
      {
        "type": "record",
        "name": "IncomePredictionResult",
        "namespace": "loan",
        "fields": [
          {"name": "requestId", "type": "string"},
          {"name": "applicationId", "type": "string"},
          {"name": "customerId", "type": "int"},
          {"name": "prospectId", "type": "int"},
          {"name": "requestedAt", "type": "long"},
          {"name": "incomeSource", "type": ["null", "string"]},
          {"name": "isCustomer", "type": "boolean"},
          {"name": "predictedIncome", "type": "double"},
          {"name": "processedAt", "type": "long"},
          {"name": "sentToNpl", "type": "boolean"}
        ]
      }
    """

      override protected def getToGenericRecord: (PredictionResultRecord, GenericRecord) => Unit = (record, avroRecord) => {
        avroRecord.put("requestId", record.requestId)
        avroRecord.put("applicationId", record.applicationId)
        avroRecord.put("customerId", record.customerId)
        avroRecord.put("prospectId", record.prospectId)
        avroRecord.put("requestedAt", record.requestedAt)
        avroRecord.put("incomeSource", record.incomeSource)
        avroRecord.put("isCustomer", record.isCustomer)
        avroRecord.put("predictedIncome", record.predictedIncome)
        avroRecord.put("processedAt", record.processedAt)
        avroRecord.put("sentToNpl", record.sentToNpl)
      }

      override protected def getKeyExtractor: Option[PredictionResultRecord => Array[Byte]] =
        Some(record => record.requestId.getBytes())

      override protected def getKafkaProperties: Properties = {
        val props = new Properties()
        props.setProperty("bootstrap.servers", "localhost:9092")
        props.setProperty("transaction.timeout.ms", "5000")
        props.setProperty("retention.ms", "-1")  // infinite retention
        props.setProperty("cleanup.policy", "delete")
        props
      }

      override protected def getTypeInformation: TypeInformation[PredictionResultRecord] = {
        TypeInformation.of(classOf[PredictionResultRecord])
      }

    }

  def main(args: Array[String]): Unit = {
    // Create futures for consumer and producer
    val consumerFuture = Future {
      new IncomePredictionConsumer().execute()
    }

    val producerFuture = Future {
      // Directly create and execute the producer
      new IncomePredictionResultProducer().execute()
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
}