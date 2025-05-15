package loan

import generators.{predictionRequest, predictionRequestGenerator}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import source.StreamProducer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

object loanDecisionService {

  /*********** event1 - Producer ************/

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
      |    {"name": "incomeSource", "type": ["null","string"], "default": null},
      |    {"name": "isCustomer", "type": "boolean"}
      |  ]
      |}
    """.stripMargin

  // Implementation for Income Prediction Producer
  class IncomePredictionProducer extends StreamProducer[predictionRequest, predictionRequest](
    keySelector = _.requestId.getOrElse(""),
    sleepMillisPerEvent = 500
  )(implicitly[TypeInformation[predictionRequest]], implicitly[TypeInformation[predictionRequest]]) {
    override protected def schemaString: String = SCHEMA_STRING_INCOME_PREDICTION
    override protected def topicName: String = "income_prediction_request"
    override protected def printEnabled: Boolean = true
    override protected def convertSourceToTarget(source: predictionRequest): predictionRequest = source
    override protected def createSourceGenerator(): SourceFunction[predictionRequest] = new predictionRequestGenerator(sleepMillisPerEvent, isCustomerParameter=false)
    override protected def toGenericRecord(element: predictionRequest, record: GenericRecord): Unit = {
      record.put("requestId", element.requestId.orNull)
      record.put("applicationId", element.applicationId.orNull)
      record.put("customerId", element.customerId.getOrElse(0))
      record.put("prospectId", element.prospectId.getOrElse(0))
      record.put("incomeRequestedAt", element.incomeRequestedAt.getOrElse(0L))
      record.put("e1ProducedAt", element.e1ProducedAt.getOrElse(0L))
      record.put("incomeSource", element.incomeSource)
      record.put("isCustomer", element.isCustomer)
    }
  }

  /*********** event3 - Producer ************/

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
      |    {"name": "e1ProducedAt", "type": "long"},
      |    {"name": "incomeSource", "type": ["null","string"], "default": null},
      |    {"name": "isCustomer", "type": "boolean"}
      |    {"name": "predictedIncome", "type": "double"}
      |  ]
      |}
    """.stripMargin

  // Implementation for NPL Producer
  class NplPredictionProducer extends StreamProducer[predictionRequest, predictionRequest](
    keySelector = _.customerId.getOrElse(0).toString,
    sleepMillisPerEvent = 125
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
      record.put("e1ProducedAt", element.e1ProducedAt.getOrElse(0L))
      record.put("incomeSource", element.incomeSource)
      record.put("isCustomer", element.isCustomer)
      record.put("predictedIncome", element.predictedIncome.getOrElse(0))
    }
  }

  // main method runs the producers in parallel
  def main(args: Array[String]): Unit = {
    // Create futures for producers
    val producer1Future = Future {
      new IncomePredictionProducer().produce()
    }

    val producer2Future = Future {
      new NplPredictionProducer().produce()
    }

    // Run two basic producers
    val combinedFuture = Future.sequence(Seq(producer1Future, producer2Future))

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