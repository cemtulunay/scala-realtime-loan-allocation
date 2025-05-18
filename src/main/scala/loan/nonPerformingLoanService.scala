package utils

import loan.utils.{randomCreditScore, skewedRandom}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.jdbc.JdbcStatementBuilder
import source.AbstractPostgreSQLToKafkaProducer
import target.StreamConsumer

import java.sql.{PreparedStatement, ResultSet}
import java.util.Properties
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object nonPerformingLoanService {


  /*******************************************************************************/
  /**************************** event2 - Consumer ********************************/
  /*******************************************************************************/


  // Case class for npl prediction record
  case class PredictionResultRecord(
                                     requestId: String,
                                     applicationId: String,
                                     customerId: Int,
                                     prospectId: Int,
                                     incomeRequestedAt: Long,
                                     e1ProducedAt: Long,
                                     systemTime: Long,
                                     incomeSource: String,
                                     kafkaSource: String,
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
      |    {"name": "systemTime", "type": "long"},
      |    {"name": "incomeSource", "type": ["null", "string"]},
      |    {"name": "isCustomer", "type": "boolean"},
      |    {"name": "kafkaSource", "type": "string"},
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
          income_requested_at, npl_requested_at, e1_produced_at, system_time, income_source,
          is_customer, kafka_source, predicted_income, e1_consumed_at, sent_to_npl,
          e2_produced_at, e2_consumed_at, credit_score, sent_to_ldes, predicted_npl
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, false, ?)
        ON CONFLICT (request_id)
        DO UPDATE SET
        credit_score = EXCLUDED.credit_score,
        predicted_npl = EXCLUDED.predicted_npl,
        npl_requested_at = EXCLUDED.income_requested_at + (extract(epoch from current_timestamp) * 1000)::bigint - EXCLUDED.system_time,
        e2_consumed_at = EXCLUDED.income_requested_at + (extract(epoch from current_timestamp) * 1000)::bigint - EXCLUDED.system_time,
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
          statement.setLong(6, record.incomeRequestedAt + (System.currentTimeMillis() - record.systemTime))
          statement.setLong(7, record.e1ProducedAt)
          statement.setLong(8, record.systemTime)
          statement.setString(9, record.incomeSource)
          statement.setBoolean(10, record.isCustomer)
          statement.setString(11, record.kafkaSource)
          statement.setDouble(12, record.predictedIncome)
          statement.setLong(13, record.e1ConsumedAt)
          statement.setBoolean(14, record.sentToNpl)
          statement.setLong(15, record.e2ProducedAt)
          statement.setLong(16, record.incomeRequestedAt + (System.currentTimeMillis() - record.systemTime))
          statement.setDouble(17, record.creditScore)
          statement.setDouble(18, record.predictedNpl)
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
              systemTime = record.get("systemTime").toString.toLong,
              incomeSource = Option(record.get("incomeSource")).map(_.toString).orNull,
              isCustomer = Option(record.get("isCustomer")).exists(_.toString.toBoolean),
              kafkaSource = record.get("kafkaSource").toString,
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


  /*******************************************************************************/
  /**************************** event3 - Consumer ********************************/
  /*******************************************************************************/


  // Case class for npl prediction record
  case class NplPredictionRequestRecord(
                                     requestId: String,
                                     applicationId: String,
                                     customerId: Int,
                                     prospectId: Int,
                                     incomeRequestedAt: Long,
                                     nplRequestedAt: Long,
                                     e3ProducedAt: Long,
                                     systemTime: Long,
                                     incomeSource: String,
                                     isCustomer: Boolean,
                                     predictedIncome: Double,
                                     creditScore: Int,
                                     predictedNpl: Double
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
        |    {"name": "incomeRequestedAt", "type": "long"},
        |    {"name": "nplRequestedAt", "type": "long"},
        |    {"name": "e3ProducedAt", "type": "long"},
        |    {"name": "systemTime", "type": "long"},
        |    {"name": "incomeSource", "type": ["null","string"], "default": null},
        |    {"name": "isCustomer", "type": "boolean"},
        |    {"name": "predictedIncome", "type": "double"}
        |  ]
        |}
    """.stripMargin,
    consumerGroupId = "npl_prediction_request_consumer",
    printToConsole = false
  ) {

    override protected def getSqlInsertStatement(): String = {
      """
        INSERT INTO npl_prediction (
          request_id, application_id, customer_id, prospect_id,
          income_requested_at, npl_requested_at, e3_produced_at, system_time,
          income_source, is_customer, kafka_source, predicted_income,
          e3_consumed_at, sent_to_npl, credit_score, sent_to_ldes, predicted_npl
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'npl_prediction_request', ?, ?, true, ?, false, ?)
        ON CONFLICT (request_id)
        DO UPDATE SET
          credit_score = EXCLUDED.credit_score,
          predicted_npl = EXCLUDED.predicted_npl,
          e3_consumed_at = EXCLUDED.npl_requested_at + (extract(epoch from current_timestamp) * 1000)::bigint - EXCLUDED.system_time,
          sent_to_ldes = false
        """.stripMargin
    }

    override protected def createJdbcStatementBuilder(): JdbcStatementBuilder[NplPredictionRequestRecord] = {
      new JdbcStatementBuilder[NplPredictionRequestRecord] {
        override def accept(statement: PreparedStatement, record: NplPredictionRequestRecord): Unit = {
          statement.setString(1, record.requestId)
          statement.setString(2, record.applicationId)
          statement.setInt(3, record.customerId)
          statement.setInt(4, record.prospectId)
          statement.setLong(5, record.incomeRequestedAt)
          statement.setLong(6, record.nplRequestedAt)
          statement.setLong(7, record.e3ProducedAt)
          statement.setLong(8, record.systemTime)
          statement.setString(9, record.incomeSource)
          statement.setBoolean(10, record.isCustomer)
          statement.setDouble(11, record.predictedIncome)
          statement.setLong(12, record.nplRequestedAt + (System.currentTimeMillis() - record.systemTime))
          statement.setInt(13, record.creditScore)
          statement.setDouble(14, record.predictedNpl)
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
              incomeRequestedAt = record.get("incomeRequestedAt").toString.toLong,
              nplRequestedAt = record.get("nplRequestedAt").toString.toLong,
              e3ProducedAt = record.get("e3ProducedAt").toString.toLong,
              systemTime = record.get("systemTime").toString.toLong,
              incomeSource = Option(record.get("incomeSource")).map(_.toString).orNull,
              isCustomer = Option(record.get("isCustomer")).exists(_.toString.toBoolean),
              predictedIncome = record.get("predictedIncome").toString.toDouble,
              creditScore = randomCreditScore(),
              predictedNpl = skewedRandom()
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


  /*******************************************************************************/
  /**************************** event4 - Producer ********************************/
  /*******************************************************************************/


  // Define the case class for the original use case
  case class NplPredictionResultRecord (
                                     requestId: String,
                                     applicationId: String,
                                     customerId: Int,
                                     prospectId: Int,
                                     incomeRequestedAt: Long,
                                     e1ProducedAt: Long,
                                     systemTime: Long,
                                     incomeSource: String,
                                     isCustomer: Boolean,
                                     kafkaSource: String,
                                     predictedIncome: Double,
                                     e1ConsumedAt: Long,
                                     sentToNpl: Boolean,
                                     e2ProducedAt: Long,
                                     e2ConsumedAt: Long,
                                     nplRequestedAt: Long,
                                     e3ProducedAt: Long,
                                     e3ConsumedAt: Long,
                                     creditScore: Long,
                                     sentToLdes: Boolean,
                                     e4ProducedAt: Long,
                                     predictedNpl: Double
                                   ) extends Serializable

  /**
   * Concrete implementation of AbstractPostgreSQLToKafkaProducer for income prediction results
   */
  class NplPredictionResultProducer extends AbstractPostgreSQLToKafkaProducer[NplPredictionResultRecord] {

    override protected def getJobName: String = "NPL Prediction Result Producer"
    override protected def getJdbcUrl: String = "jdbc:postgresql://localhost:5432/loan_db"
    override protected def getJdbcUsername: String = "docker"
    override protected def getJdbcPassword: String = "docker"
    override protected def getSelectQuery: String = """
      SELECT
        request_id, application_id, customer_id, prospect_id,
        income_requested_at, e1_produced_at, system_time, income_source,
        is_customer, kafka_source, predicted_income, e1_consumed_at,
        sent_to_npl, e2_produced_at, e2_consumed_at, npl_requested_at,
        e3_produced_at, e3_consumed_at, credit_score, sent_to_ldes,
        predicted_npl
      FROM npl_prediction
      WHERE sent_to_ldes = false
      ORDER BY e2_consumed_at ASC, e3_consumed_at ASC
      """
    override protected def getUpdateQuery: Option[String] = Some("UPDATE npl_prediction SET sent_to_ldes = true, e4_produced_at = ? WHERE request_id = ?")
    override protected def getUpdateQueryParamSetter: Option[(PreparedStatement, NplPredictionResultRecord) => Unit] = Some((stmt, record) => {
      stmt.setLong(1, record.e4ProducedAt)
      stmt.setString(2, record.requestId)
    })
    override protected def getRecordMapper: ResultSet => NplPredictionResultRecord = rs => {
      val systemTime2 = System.currentTimeMillis()

      NplPredictionResultRecord(
        requestId = rs.getString("request_id"),
        applicationId = rs.getString("application_id"),
        customerId = rs.getInt("customer_id"),
        prospectId = rs.getInt("prospect_id"),
        incomeRequestedAt = rs.getLong("income_requested_at"),
        e1ProducedAt = rs.getLong("e1_produced_at"),
        systemTime = rs.getLong("system_time"),
        incomeSource = rs.getString("income_source"),
        isCustomer = rs.getBoolean("is_customer"),
        kafkaSource = rs.getString("kafka_source"),
        predictedIncome = rs.getDouble("predicted_income"),
        e1ConsumedAt = rs.getLong("e1_consumed_at"),
        sentToNpl = rs.getBoolean("sent_to_npl"),
        e2ProducedAt = rs.getLong("e2_produced_at"),
        e2ConsumedAt = rs.getLong("e2_consumed_at"),
        nplRequestedAt = rs.getLong("npl_requested_at"),
        e3ProducedAt = rs.getLong("e3_produced_at"),
        e3ConsumedAt = rs.getLong("e3_consumed_at"),
        creditScore = rs.getLong("credit_score"),
        sentToLdes =  true,
        e4ProducedAt = if (rs.getBoolean("is_customer")) rs.getLong("npl_requested_at") + (systemTime2 - rs.getLong("system_time")) else rs.getLong("income_requested_at") + (systemTime2 - rs.getLong("system_time")),
        predictedNpl = rs.getDouble("predicted_npl"),
      )
    }
    override protected def getKafkaTopic: String = "npl_prediction_result"
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
          {"name": "systemTime", "type": "long"},
          {"name": "incomeSource", "type": ["null", "string"]},
          {"name": "isCustomer", "type": "boolean"},
          {"name": "kafkaSource", "type": "string"},
          {"name": "predictedIncome", "type": "double"},
          {"name": "e1ConsumedAt", "type": "long"},
          {"name": "sentToNpl", "type": "boolean"},
          {"name": "e2ProducedAt", "type": "long"},
          {"name": "e2ConsumedAt", "type": "long"},
          {"name": "nplRequestedAt", "type": "long"},
          {"name": "e3ProducedAt", "type": "long"},
          {"name": "e3ConsumedAt", "type": "long"},
          {"name": "creditScore", "type": "long"},
          {"name": "sentToLdes", "type": "boolean"},
          {"name": "e4ProducedAt", "type": "long"},
          {"name": "predictedNpl", "type": "double"}
        ]
      }
    """

    override protected def getToGenericRecord: (NplPredictionResultRecord, GenericRecord) => Unit = (record, avroRecord) => {
      avroRecord.put("requestId", record.requestId)
      avroRecord.put("applicationId", record.applicationId)
      avroRecord.put("customerId", record.customerId)
      avroRecord.put("prospectId", record.prospectId)
      avroRecord.put("incomeRequestedAt", record.incomeRequestedAt)
      avroRecord.put("e1ProducedAt", record.e1ProducedAt)
      avroRecord.put("systemTime", record.systemTime)
      avroRecord.put("incomeSource", record.incomeSource)
      avroRecord.put("isCustomer", record.isCustomer)
      avroRecord.put("kafkaSource", record.kafkaSource)
      avroRecord.put("predictedIncome", record.predictedIncome)
      avroRecord.put("e1ConsumedAt", record.e1ConsumedAt)
      avroRecord.put("sentToNpl", record.sentToNpl)
      avroRecord.put("e2ProducedAt", record.e2ProducedAt)
      avroRecord.put("e2ConsumedAt", record.e2ConsumedAt)
      avroRecord.put("nplRequestedAt", record.nplRequestedAt)
      avroRecord.put("e3ProducedAt", record.e3ProducedAt)
      avroRecord.put("e3ConsumedAt", record.e3ConsumedAt)
      avroRecord.put("creditScore", record.creditScore)
      avroRecord.put("sentToLdes", record.sentToLdes)
      avroRecord.put("e4ProducedAt", record.e4ProducedAt)
      avroRecord.put("predictedNpl", record.predictedNpl)
    }

    override protected def getKeyExtractor: Option[NplPredictionResultRecord => Array[Byte]] =
      Some(record => record.requestId.getBytes())

    override protected def getKafkaProperties: Properties = {
      val props = new Properties()
      props.setProperty("bootstrap.servers", "localhost:9092")
      props.setProperty("transaction.timeout.ms", "5000")
      props.setProperty("retention.ms", "-1")  // infinite retention
      props.setProperty("cleanup.policy", "delete")
      props
    }

    override protected def getTypeInformation: TypeInformation[NplPredictionResultRecord] = {
      TypeInformation.of(classOf[NplPredictionResultRecord])
    }

  }


  def main(args: Array[String]): Unit = {
    // Create futures for consumer and producer
    val consumerFuture1 = Future {
      new IncomePredictionResultConsumer().execute()
    }

    val consumerFuture2 = Future {
      // Directly create and execute the producer
      new NplPredictionConsumer().execute()
    }

    val producerFuture1 = Future {
      // Directly create and execute the producer
      new NplPredictionResultProducer().execute()
    }

    // Run both processes
    val combinedFuture = Future.sequence(Seq(consumerFuture1, consumerFuture2, producerFuture1))

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
