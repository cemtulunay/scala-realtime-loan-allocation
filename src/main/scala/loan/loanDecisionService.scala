package loan

import generators.{predictionRequest, predictionRequestGenerator}
import loan.incomePredictionService.GenericRecordKryoSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.ProducerRecord

import java.io.ByteArrayOutputStream
import java.util.Properties
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration


object loanDecisionService {

  class GenericAvroSerializer[T](
                                  schemaString: String,  // Pass schema as string instead of Schema object
                                  topic: String,
                                  toGenericRecord: (T, GenericRecord) => Unit
                                ) extends KafkaSerializationSchema[T] with Serializable {

    @transient private lazy val schema: Schema = new Schema.Parser().parse(schemaString)

    override def serialize(element: T, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
      val byteArrayOutputStream = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().blockingBinaryEncoder(byteArrayOutputStream, null)
      val writer = new GenericDatumWriter[GenericRecord](schema)

      val record = new GenericData.Record(schema)
      toGenericRecord(element, record)

      writer.write(record, encoder)
      encoder.flush()

      val avroBytes = byteArrayOutputStream.toByteArray
      byteArrayOutputStream.close()

      new ProducerRecord[Array[Byte], Array[Byte]](
        topic,
        null,
        avroBytes
      )
    }
  }

  abstract class StreamProducer(sleepMillisPerEvent: Int = 100,
                                kafkaBootstrapServers: String = "localhost:9092",
                                kafkaTransactionTimeout: Int = 5000,
                                keySelector: predictionRequest => String = _.customerId.getOrElse(0).toString,
                                flinkSemantic: FlinkKafkaProducer.Semantic = FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                               ) extends Serializable {
    // Abstract members
    protected def schemaString: String
    protected def topicName: String
    protected def printEnabled: Boolean = false
    protected def toGenericRecord(element: predictionRequest, record: GenericRecord): Unit

    // Create components in produce() method instead of class level
    def produce(): Unit = {
      // 1-) Setup Environment and add Data generator as a Source
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val sourceGenerator = new predictionRequestGenerator(sleepMillisPerEvent)

      val kafkaProps = new Properties()
      kafkaProps.setProperty("bootstrap.servers", kafkaBootstrapServers)
      kafkaProps.setProperty("transaction.timeout.ms", kafkaTransactionTimeout.toString)

      val serializer = createSerializer()

      val sourceEvents = env.addSource(sourceGenerator)
      val eventsPerRequest = sourceEvents.keyBy(keySelector)

      // 2-) Create Event Stream
      /** ****************************************************************************************************************************************************
       *  STATEFUL APPROACH - State Primitives - ValueState - Distributed Available - Not Necessary, used for showcasing
       * **************************************************************************************************************************************************** */
      val processedRequests = eventsPerRequest.process(
        new KeyedProcessFunction[String, predictionRequest, predictionRequest] {
          @transient private var stateCounter: ValueState[Long] = _

          override def open(parameters: Configuration): Unit = {
            val descriptor = new ValueStateDescriptor[Long]("events-counter", classOf[Long])
            stateCounter = getRuntimeContext.getState(descriptor)
          }

          override def processElement(
                                       value: predictionRequest,
                                       ctx: KeyedProcessFunction[String, predictionRequest, predictionRequest]#Context,
                                       out: Collector[predictionRequest]
                                     ): Unit = {
            val currentState = Option(stateCounter.value()).getOrElse(0L)
            stateCounter.update(currentState + 1)
            out.collect(value)
          }
        }
      )

      // 3-) Instantiate Kafka Producer with Avro serialization
      val kafkaProducer = new FlinkKafkaProducer[predictionRequest](
        topicName,
        serializer,
        kafkaProps,
        flinkSemantic
      )

      // 4-) Pass Stream to Kafka Producer and print to console if required
      if (printEnabled) {
        processedRequests.print()
      }
      processedRequests.addSink(kafkaProducer)

      env.execute(s"Stream Producer for $topicName")
    }

    private def createSerializer(): GenericAvroSerializer[predictionRequest] = {
      new GenericAvroSerializer[predictionRequest](
        schemaString = schemaString,
        topic = topicName,
        toGenericRecord = toGenericRecord
      )
    }
  }



  // Create Avro schema for incomePredictionRequest
  val SCHEMA_STRING_INCOME_PREDICTION =
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
    """.stripMargin


  // Implementation for Income Prediction Producer
  class IncomePredictionProducer extends StreamProducer with Serializable {
    override protected def schemaString: String = SCHEMA_STRING_INCOME_PREDICTION
    override protected def topicName: String = "income_prediction_request"
    override protected def printEnabled: Boolean = false

    override protected def toGenericRecord(element: predictionRequest, record: GenericRecord): Unit = {
      record.put("requestId", element.requestId.orNull)
      record.put("applicationId", element.applicationId.orNull)
      record.put("customerId", element.customerId.getOrElse(0))
      record.put("prospectId", element.prospectId.getOrElse(0))
      record.put("requestedAt", element.requestedAt.getOrElse(0L))
      record.put("incomeSource", element.incomeSource)
      record.put("isCustomer", element.isCustomer)
    }
  }

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
      |    {"name": "requestedAt", "type": "long"},
      |    {"name": "incomeSource", "type": ["null","string"], "default": null},
      |    {"name": "isCustomer", "type": ["null","boolean"], "default": null},
      |    {"name": "predictedIncome", "type": "double"}
      |  ]
      |}
    """.stripMargin

  // Implementation for NPL Producer
  class NplPredictionProducer extends StreamProducer with Serializable {
    override protected def schemaString: String = SCHEMA_STRING_NPL_PREDICTION
    override protected def topicName: String = "npl_prediction_request"
    override protected def printEnabled: Boolean = true

    override protected def toGenericRecord(element: predictionRequest, record: GenericRecord): Unit = {
      record.put("requestId", element.requestId.orNull)
      record.put("applicationId", element.applicationId.orNull)
      record.put("customerId", element.customerId.getOrElse(0))
      record.put("prospectId", element.prospectId.getOrElse(0))
      record.put("requestedAt", element.requestedAt.getOrElse(0L))
      record.put("incomeSource", element.incomeSource)
      record.put("isCustomer", element.isCustomer)
      record.put("predictedIncome", element.predictedIncome.getOrElse(0))
    }
  }

  // main method runs the producers and consumers in parallel
  def main(args: Array[String]): Unit = {
    // Create futures for both producers
    val producer1Future = Future {
      new NplPredictionProducer().produce()
    }

    val producer2Future = Future {
      new IncomePredictionProducer().produce()
    }

    // Combine futures and wait for both to complete
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