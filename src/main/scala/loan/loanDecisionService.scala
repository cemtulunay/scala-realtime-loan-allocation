package utils

import generators.{predictionRequest, predictionRequestGenerator}
import loan.utils.IndustryMapping.{calculateLoanAmount, getDebtToIncomeRatio, getEmploymentStability, makeLoanDecision}
import loan.utils.{IndustryMapping, randomCreditScore, skewedRandom}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.jdbc.JdbcStatementBuilder
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import source.{AbstractPostgreSQLToKafkaProducer, StreamProducer}
import target.StreamConsumer

import java.sql.{PreparedStatement, ResultSet}
import java.util.Properties
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

object loanDecisionService {


  /*******************************************************************************/
  /**************************** event1 - Producer ********************************/
  /*******************************************************************************/


  // Avro schemas
  val SCHEMA_STRING_INCOME_PREDICTION =
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
      |    {"name": "systemTime", "type": "long"},
      |    {"name": "incomeSource", "type": ["null","string"], "default": null},
      |    {"name": "isCustomer", "type": "boolean"}
      |  ]
      |}
    """.stripMargin

  // Implementation for Income Prediction Producer
  class IncomePredictionProducer extends StreamProducer[predictionRequest, predictionRequest](
    keySelector = _.requestId.getOrElse(""),
    sleepMillisPerEvent = 2500
  )(implicitly[TypeInformation[predictionRequest]], implicitly[TypeInformation[predictionRequest]]) {
    override protected def schemaString: String = SCHEMA_STRING_INCOME_PREDICTION
    override protected def topicName: String = "income_prediction_request"
    override protected def printEnabled: Boolean = false
    override protected def convertSourceToTarget(source: predictionRequest): predictionRequest = source
    override protected def createSourceGenerator(): SourceFunction[predictionRequest] = new predictionRequestGenerator(sleepMillisPerEvent, isCustomerParameter=false)
    override protected def toGenericRecord(element: predictionRequest, record: GenericRecord): Unit = {
      record.put("requestId", element.requestId.orNull)
      record.put("applicationId", element.applicationId.orNull)
      record.put("customerId", element.customerId.getOrElse(0))
      record.put("prospectId", element.prospectId.getOrElse(0))
      record.put("incomeRequestedAt", element.incomeRequestedAt.getOrElse(0L))
      record.put("e1ProducedAt", element.e1ProducedAt.getOrElse(0L))
      record.put("systemTime", element.systemTime.getOrElse(0L))
      record.put("incomeSource", element.incomeSource)
      record.put("isCustomer", element.isCustomer)
    }
  }


  /*******************************************************************************/
  /**************************** event3 - Producer ********************************/
  /*******************************************************************************/


  val SCHEMA_STRING_NPL_PREDICTION =
    """
      |{
      |  "type": "record",
      |  "name": "NplPredictionResult",
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
      |
      |
      |  ]
      |}
    """.stripMargin

  // Implementation for NPL Producer
  class NplPredictionProducer extends StreamProducer[predictionRequest, predictionRequest](
    keySelector = _.customerId.getOrElse(0).toString,
    sleepMillisPerEvent = 500
  )(implicitly[TypeInformation[predictionRequest]], implicitly[TypeInformation[predictionRequest]]) {
    override protected def schemaString: String = SCHEMA_STRING_NPL_PREDICTION
    override protected def topicName: String = "npl_prediction_request"
    override protected def printEnabled: Boolean = false
    override protected def convertSourceToTarget(source: predictionRequest): predictionRequest = source
    override protected def createSourceGenerator(): SourceFunction[predictionRequest] = new predictionRequestGenerator(sleepMillisPerEvent)
    override protected def toGenericRecord(element: predictionRequest, record: GenericRecord): Unit = {
      record.put("requestId", element.requestId.orNull)
      record.put("applicationId", element.applicationId.orNull)
      record.put("customerId", element.customerId.getOrElse(0))
      record.put("prospectId", element.prospectId.getOrElse(0))
      record.put("incomeRequestedAt", element.incomeRequestedAt.getOrElse(0L))
      record.put("nplRequestedAt", element.nplRequestedAt.getOrElse(0L))
      record.put("e3ProducedAt", element.nplRequestedAt.getOrElse(0L))
      record.put("systemTime", element.systemTime.getOrElse(0L))
      record.put("incomeSource", element.incomeSource)
      record.put("isCustomer", element.isCustomer)
      record.put("predictedIncome", element.predictedIncome.getOrElse(0))
    }
  }


  /*******************************************************************************/
  /**************************** event4 - Consumer ********************************/
  /*******************************************************************************/


  // Case class for npl prediction record
  case class LoanResultRecord(
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
                               predictedNpl: Double,
                               debtToIncomeRatio: Double,
                               employmentIndustry: String,
                               employmentStability: Double,
                               loanDecision: Boolean,
                               loanAmount: Double
                             ) extends Serializable

  // Implementation of the abstract class for income prediction
  class LoanResultConsumer extends StreamConsumer[LoanResultRecord](
    topicName = "npl_prediction_result",
    schemaString =
      """
        |{
        |  "type": "record",
        |  "name": "NplPredictionResult",
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
        |    {"name": "e2ProducedAt", "type": "long"},
        |    {"name": "e2ConsumedAt", "type": "long"},
        |    {"name": "nplRequestedAt", "type": "long"},
        |    {"name": "e3ProducedAt", "type": "long"},
        |    {"name": "e3ConsumedAt", "type": "long"},
        |    {"name": "creditScore", "type": "long"},
        |    {"name": "sentToLdes", "type": "boolean"},
        |    {"name": "e4ProducedAt", "type": "long"},
        |    {"name": "predictedNpl", "type": "double"}
        |  ]
        |}
    """.stripMargin,
    consumerGroupId = "npl_prediction_result_consumer",
    printToConsole = false
  ) {

    override protected def getSqlInsertStatement(): String = {
      """
        INSERT INTO loan_result (
          request_id, application_id, customer_id, prospect_id,
          income_requested_at, e1_produced_at, system_time, income_source,
          is_customer, kafka_source, predicted_income, e1_consumed_at,
          sent_to_npl, e2_produced_at, e2_consumed_at, npl_requested_at,
          e3_produced_at, e3_consumed_at, credit_score, sent_to_ldes,
          e4_produced_at, predicted_npl, e4_consumed_at, debt_to_income_ratio,
          employment_industry, employment_stability, loan_decision, sent_to_ns,
          sent_to_ldis, loan_amount
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, false, false, ?)
        ON CONFLICT (request_id)
        DO UPDATE SET
        e4_consumed_at = CASE
          WHEN EXCLUDED.is_customer
          THEN EXCLUDED.npl_requested_at + (extract(epoch from current_timestamp) * 1000)::bigint - EXCLUDED.system_time
          ELSE EXCLUDED.income_requested_at + (extract(epoch from current_timestamp) * 1000)::bigint - EXCLUDED.system_time
        END,
        debt_to_income_ratio = EXCLUDED.debt_to_income_ratio,
        employment_industry = EXCLUDED.employment_industry,
        employment_stability = EXCLUDED.employment_stability,
        loan_decision = EXCLUDED.loan_decision,
        loan_amount = EXCLUDED.loan_amount,
        sent_to_ns = false,
        sent_to_ldis = false
        """.stripMargin
    }

    override protected def createJdbcStatementBuilder(): JdbcStatementBuilder[LoanResultRecord] = {
      new JdbcStatementBuilder[LoanResultRecord] {
        override def accept(statement: PreparedStatement, record: LoanResultRecord): Unit = {
          statement.setString(1, record.requestId)
          statement.setString(2, record.applicationId)
          statement.setInt(3, record.customerId)
          statement.setInt(4, record.prospectId)
          statement.setLong(5, record.incomeRequestedAt)
          statement.setLong(6, record.e1ProducedAt)
          statement.setLong(7, record.systemTime)
          statement.setString(8, record.incomeSource)
          statement.setBoolean(9, record.isCustomer)
          statement.setString(10, record.kafkaSource)
          statement.setDouble(11, record.predictedIncome)
          statement.setLong(12, record.e1ConsumedAt)
          statement.setBoolean(13, record.sentToNpl)
          statement.setLong(14, record.e2ProducedAt)
          statement.setLong(15, record.e2ConsumedAt)
          statement.setLong(16, record.nplRequestedAt)
          statement.setLong(17, record.e3ProducedAt)
          statement.setLong(18, record.e3ConsumedAt)
          statement.setDouble(19, record.creditScore)
          statement.setBoolean(20, record.sentToLdes)
          statement.setLong(21, record.e4ProducedAt)
          statement.setDouble(22, record.predictedNpl)
          if (record.isCustomer) {
            statement.setLong(23, record.nplRequestedAt + (System.currentTimeMillis() - record.systemTime))
          } else {
            statement.setLong(23, record.incomeRequestedAt + (System.currentTimeMillis() - record.systemTime))
          }
          statement.setDouble(24, record.debtToIncomeRatio)
          statement.setString(25, record.employmentIndustry)
          statement.setDouble(26, record.employmentStability)
          statement.setBoolean(27, record.loanDecision)
          statement.setDouble(28, record.loanAmount)
        }
      }
    }

    override protected def createRecordMapper(): MapFunction[GenericRecord, LoanResultRecord] = {
      new MapFunction[GenericRecord, LoanResultRecord] {
        override def map(record: GenericRecord): LoanResultRecord = {
          try {
            val customerId = record.get("customerId").toString.toInt
            val industry = IndustryMapping.getRandomIndustry()
            val debtRatio = getDebtToIncomeRatio(industry)
            val empStability = getEmploymentStability(industry)
            val isLoanApproved = if (makeLoanDecision(
              record.get("predictedIncome").toString.toDouble,
              record.get("creditScore").toString.toInt,
              record.get("predictedNpl").toString.toDouble,
              industry,
              debtRatio,
              empStability
            ) == "APPROVED") true else false
            val loanAmount = calculateLoanAmount(debtRatio, record.get("predictedIncome").toString.toDouble)

            LoanResultRecord(
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
              e2ConsumedAt = record.get("e2ConsumedAt").toString.toLong,
              nplRequestedAt = record.get("nplRequestedAt").toString.toLong,
              e3ProducedAt = record.get("e3ProducedAt").toString.toLong,
              e3ConsumedAt = record.get("e3ConsumedAt").toString.toLong,
              creditScore = record.get("creditScore").toString.toLong,
              sentToLdes = Option(record.get("sentToLdes")).exists(_.toString.toBoolean),
              e4ProducedAt = record.get("e4ProducedAt").toString.toLong,
              predictedNpl = record.get("predictedNpl").toString.toDouble,
              debtToIncomeRatio = debtRatio,
              employmentIndustry = industry,
              employmentStability = empStability,
              loanDecision = isLoanApproved,
              loanAmount = loanAmount
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
  /**************************** event5 - Producer ********************************/
  /*******************************************************************************/


  // Define the case class for the original use case
  case class LoanDecisionNotificationRecord (
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

  /**
   * Concrete implementation of AbstractPostgreSQLToKafkaProducer for income prediction results
   */
  class NplPredictionResultProducer extends AbstractPostgreSQLToKafkaProducer[LoanDecisionNotificationRecord] {

    override protected def getJobName: String = "Loan Decision Producer"
    override protected def getJdbcUrl: String = "jdbc:postgresql://localhost:5432/loan_db"
    override protected def getJdbcUsername: String = "docker"
    override protected def getJdbcPassword: String = "docker"
    override protected def getSelectQuery: String = """
      SELECT
        request_id, application_id, customer_id, prospect_id,
        income_requested_at, system_time, is_customer, npl_requested_at,
        credit_score, predicted_npl, debt_to_income_ratio, loan_decision,
        loan_amount, sent_to_ns
      FROM loan_result
      WHERE sent_to_ns = false
      ORDER BY e4_consumed_at ASC
      """
    override protected def getUpdateQuery: Option[String] = Some("UPDATE loan_result SET sent_to_ldes = true, e5_produced_at = ? WHERE request_id = ?")
    override protected def getUpdateQueryParamSetter: Option[(PreparedStatement, LoanDecisionNotificationRecord) => Unit] = Some((stmt, record) => {
      stmt.setLong(1, record.e5ProducedAt)
      stmt.setString(2, record.requestId)
    })
    override protected def getRecordMapper: ResultSet => LoanDecisionNotificationRecord = rs => {
      val systemTime2 = System.currentTimeMillis()

      LoanDecisionNotificationRecord(
        requestId = rs.getString("request_id"),
        applicationId = rs.getString("application_id"),
        customerId = if (rs.getBoolean("is_customer")) rs.getInt("customer_id") else rs.getInt("prospect_id"),
        incomeRequestedAt = rs.getLong("income_requested_at"),
        systemTime = rs.getLong("system_time"),
        isCustomer = rs.getBoolean("is_customer"),
        nplRequestedAt = rs.getLong("npl_requested_at"),
        creditScore = rs.getLong("credit_score"),
        predictedNpl = rs.getDouble("predicted_npl"),
        debtToIncomeRatio = rs.getDouble("debt_to_income_ratio"),
        loanDecision = rs.getBoolean("loan_decision"),
        loanAmount = rs.getDouble("loan_amount"),
        e5ProducedAt = if (rs.getBoolean("is_customer")) rs.getLong("npl_requested_at") + (systemTime2 - rs.getLong("system_time")) else rs.getLong("income_requested_at") + (systemTime2 - rs.getLong("system_time"))
      )
    }
    override protected def getKafkaTopic: String = "loan_decision_result_notification"
    override protected def getAvroSchema: String = """
      {
        "type": "record",
        "name": "LoanDecisionResultNotification",
        "namespace": "loan",
        "fields": [
          {"name": "requestId", "type": "string"},
          {"name": "applicationId", "type": "string"},
          {"name": "customerId", "type": "int"},
          {"name": "incomeRequestedAt", "type": ["null", "long"], "default": null},
          {"name": "systemTime", "type": "long"},
          {"name": "isCustomer", "type": "boolean"},
          {"name": "nplRequestedAt", "type": ["null", "long"], "default": null},
          {"name": "creditScore", "type": "long"},
          {"name": "predictedNpl", "type": "double"},
          {"name": "debtToIncomeRatio", "type": "double"},
          {"name": "loanDecision", "type": "boolean"},
          {"name": "loanAmount", "type": "double"},
          {"name": "e5ProducedAt", "type": ["null", "long"], "default": null}
        ]
      }
    """

    override protected def getToGenericRecord: (LoanDecisionNotificationRecord, GenericRecord) => Unit = (record, avroRecord) => {
      avroRecord.put("requestId", record.requestId)
      avroRecord.put("applicationId", record.applicationId)
      avroRecord.put("customerId", record.customerId)
      avroRecord.put("incomeRequestedAt", record.incomeRequestedAt)
      avroRecord.put("systemTime", record.systemTime)
      avroRecord.put("isCustomer", record.isCustomer)
      avroRecord.put("nplRequestedAt", record.nplRequestedAt)
      avroRecord.put("creditScore", record.creditScore)
      avroRecord.put("predictedNpl", record.predictedNpl)
      avroRecord.put("debtToIncomeRatio", record.debtToIncomeRatio)
      avroRecord.put("loanDecision", record.loanDecision)
      avroRecord.put("loanAmount", record.loanAmount)
      avroRecord.put("e5ProducedAt", record.e5ProducedAt)
    }

    override protected def getKeyExtractor: Option[LoanDecisionNotificationRecord => Array[Byte]] =
      Some(record => record.requestId.getBytes())

    override protected def getKafkaProperties: Properties = {
      val props = new Properties()
      props.setProperty("bootstrap.servers", "localhost:9092")
      props.setProperty("transaction.timeout.ms", "5000")
      props.setProperty("retention.ms", "-1")  // infinite retention
      props.setProperty("cleanup.policy", "delete")
      props
    }

    override protected def getTypeInformation: TypeInformation[LoanDecisionNotificationRecord] = {
      TypeInformation.of(classOf[LoanDecisionNotificationRecord])
    }

  }


  /*******************************************************************************/
  /******************************** EXECUTION ************************************/
  /*******************************************************************************/


  // main method runs the producers in parallel
  def main(args: Array[String]): Unit = {
    // Create futures for producers
    val producer1Future = Future {
      new IncomePredictionProducer().produce()
    }

    val producer2Future = Future {
      new NplPredictionProducer().produce()
    }

    val consumer1Future = Future {
      new LoanResultConsumer().execute()
    }

    val producer3Future = Future {
      new NplPredictionResultProducer().execute()
    }

    // Run two basic producers
    val combinedFuture = Future.sequence(Seq(producer1Future, producer2Future, consumer1Future, producer3Future))

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