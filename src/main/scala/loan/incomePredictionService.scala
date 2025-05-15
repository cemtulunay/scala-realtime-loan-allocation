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
                               incomeRequestedAt: Long,
                               e1ProducedAt: Long,
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
        |    {"name": "requestId", "type": "string"},
        |    {"name": "applicationId", "type": "string"},
        |    {"name": "customerId", "type": "int"},
        |    {"name": "prospectId", "type": "int"},
        |    {"name": "incomeRequestedAt", "type": "long"},
        |    {"name": "e1ProducedAt", "type": "long"},
        |    {"name": "incomeSource", "type": ["null","string"], "default": null},
        |    {"name": "isCustomer", "type": "boolean"}
        |  ]
        |}
    """.stripMargin,
    consumerGroupId = "income_prediction_request_consumer"
  ) {

    override protected def getSqlInsertStatement(): String = {
      """
        |INSERT INTO income_prediction (
        |  request_id, application_id, customer_id, prospect_id,
        |  income_requested_at, e1_produced_at, income_source, is_customer,
        |  source_microservice, predicted_income, e1_consumed_at, sent_to_npl
        |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'income_prediction_request', ?, (extract(epoch from current_timestamp) * 1000)::bigint, false)
        |ON CONFLICT (request_id)
        |DO UPDATE SET
        |  predicted_income = EXCLUDED.predicted_income,
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
          statement.setLong(5, record.incomeRequestedAt)
          statement.setLong(6, record.e1ProducedAt)
          statement.setString(7, record.incomeSource)
          statement.setBoolean(8, record.isCustomer)
          statement.setDouble(9, record.predictedIncome)
        }
      }
    }

    override protected def createRecordMapper(): MapFunction[GenericRecord, PredictionRecord] = {
      new MapFunction[GenericRecord, PredictionRecord] {
        override def map(record: GenericRecord): PredictionRecord = {
          try {
            val prospectId = record.get("prospectId").toString.toInt
            PredictionRecord(
              requestId = record.get("requestId").toString,
              applicationId = record.get("applicationId").toString,
              customerId = record.get("customerId").toString.toInt,
              prospectId = prospectId,
              incomeRequestedAt = record.get("incomeRequestedAt").toString.toLong,
              e1ProducedAt = record.get("e1ProducedAt").toString.toLong,
              incomeSource = Option(record.get("incomeSource")).map(_.toString).orNull,
              isCustomer = Option(record.get("isCustomer")).exists(_.toString.toBoolean),
              predictedIncome = 50000.0 + (prospectId % 10) * 5000.0
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
                                       incomeRequestedAt: Long,
                                       e1ProducedAt: Long,
                                       incomeSource: String,
                                       isCustomer: Boolean,
                                       sourceMicroService: String,
                                       predictedIncome: Double,
                                       e1ConsumedAt: Long,
                                       sentToNpl: Boolean,
                                       e2ProducedAt: Long
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
        income_requested_at, e1_produced_at, income_source,
        is_customer, source_microservice, predicted_income,
        e1_consumed_at, sent_to_npl, e2_produced_at
      FROM income_prediction
      WHERE sent_to_npl = false
      ORDER BY e1_consumed_at ASC
    """
      override protected def getUpdateQuery: Option[String] = Some("UPDATE income_prediction SET sent_to_npl = true, e2_produced_at = (extract(epoch from current_timestamp) * 1000)::bigint WHERE request_id = ?")
      override protected def getUpdateQueryParamSetter: Option[(PreparedStatement, PredictionResultRecord) => Unit] = Some((stmt, record) => stmt.setString(1, record.requestId))
      override protected def getRecordMapper: ResultSet => PredictionResultRecord = rs =>
        PredictionResultRecord(
          requestId = rs.getString("request_id"),
          applicationId = rs.getString("application_id"),
          customerId = rs.getInt("customer_id"),
          prospectId = rs.getInt("prospect_id"),
          incomeRequestedAt = rs.getLong("income_requested_at"),
          e1ProducedAt = rs.getLong("e1_produced_at"),
          incomeSource = rs.getString("income_source"),
          isCustomer = rs.getBoolean("is_customer"),
          sourceMicroService = rs.getString("source_microservice"),
          predictedIncome = rs.getDouble("predicted_income"),
          e1ConsumedAt = rs.getLong("e1_consumed_at"),
          sentToNpl = rs.getBoolean("sent_to_npl"),
          e2ProducedAt = rs.getLong("e2_produced_at")
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
          {"name": "incomeRequestedAt", "type": "long"},
          {"name": "e1ProducedAt", "type": "long"},
          {"name": "incomeSource", "type": ["null", "string"]},
          {"name": "isCustomer", "type": "boolean"},
          {"name": "sourceMicroService", "type": "string"},
          {"name": "predictedIncome", "type": "double"},
          {"name": "e1ConsumedAt", "type": "long"},
          {"name": "sentToNpl", "type": "boolean"},
          {"name": "e2ProducedAt", "type": "long"}
        ]
      }
    """

      override protected def getToGenericRecord: (PredictionResultRecord, GenericRecord) => Unit = (record, avroRecord) => {
        avroRecord.put("requestId", record.requestId)
        avroRecord.put("applicationId", record.applicationId)
        avroRecord.put("customerId", record.customerId)
        avroRecord.put("prospectId", record.prospectId)
        avroRecord.put("incomeRequestedAt", record.incomeRequestedAt)
        avroRecord.put("e1ProducedAt", record.e1ProducedAt)
        avroRecord.put("incomeSource", record.incomeSource)
        avroRecord.put("isCustomer", record.isCustomer)
        avroRecord.put("sourceMicroService", record.sourceMicroService)
        avroRecord.put("predictedIncome", record.predictedIncome)
        avroRecord.put("e1ConsumedAt", record.e1ConsumedAt)
        avroRecord.put("sentToNpl", record.sentToNpl)
        avroRecord.put("e2ProducedAt", record.e2ProducedAt)
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