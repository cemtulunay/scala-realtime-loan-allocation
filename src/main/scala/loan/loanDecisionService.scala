package loan

import generators.{predictionRequest, predictionRequestGenerator}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.ProducerRecord

import java.io.ByteArrayOutputStream
import java.util.Properties
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

object loanDecisionService {


  /**
   * A generic Avro serializer for Kafka that can serialize any type of record.
   *
   * @param schemaString The Avro schema as a string
   * @param topic The Kafka topic to produce to
   * @param toGenericRecord Function to convert a record of type T to a GenericRecord
   * @param keyExtractor Optional function to extract a key from the record
   * @tparam T The type of records to serialize
   */
  // Generic Avro serializer
  class GenericAvroSerializer[T](
                                  schemaString: String,
                                  topic: String,
                                  toGenericRecord: (T, GenericRecord) => Unit,
                                  keyExtractor: Option[T => Array[Byte]] = None
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

      val key = keyExtractor.map(_(element)).orNull

      new ProducerRecord[Array[Byte], Array[Byte]](
        topic,
        key,
        avroBytes
      )
    }
  }

  // Fully generalized StreamProducer
  abstract class StreamProducer[S, T](
                                       protected val sleepMillisPerEvent: Int = 100,
                                       kafkaBootstrapServers: String = "localhost:9092",
                                       kafkaTransactionTimeout: Int = 5000,
                                       keySelector: T => String,
                                       flinkSemantic: FlinkKafkaProducer.Semantic = FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                                     )(implicit typeInfoS: TypeInformation[S], typeInfoT: TypeInformation[T]) extends Serializable {

    // Abstract members
    protected def schemaString: String
    protected def topicName: String
    protected def printEnabled: Boolean = false
    protected def toGenericRecord(element: T, record: GenericRecord): Unit
    protected def convertSourceToTarget(source: S): T
    protected def createSourceGenerator(): SourceFunction[S]

    // Create components in produce() method
    def produce(): Unit = {
      // 1-) Setup Environment and add Data generator as a Source
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val sourceGenerator = createSourceGenerator()

      val kafkaProps = new Properties()
      kafkaProps.setProperty("bootstrap.servers", kafkaBootstrapServers)
      kafkaProps.setProperty("transaction.timeout.ms", kafkaTransactionTimeout.toString)

      val serializer = createSerializer()

      // Convert from source type S to target type T
      val sourceEvents: DataStream[T] = env.addSource(sourceGenerator)
        .map(x => convertSourceToTarget(x))

      val eventsPerRequest = sourceEvents.keyBy(keySelector)

      // 2-) Create Event Stream with stateful processing
      /** ****************************************************************************************************************************************************
       *  STATEFUL APPROACH - State Primitives - ValueState - Distributed Available - Not Necessary, used for showcasing
       * **************************************************************************************************************************************************** */
      val processedRequests = eventsPerRequest.process(
        new KeyedProcessFunction[String, T, T] {
          @transient private var stateCounter: ValueState[Long] = _

          override def open(parameters: Configuration): Unit = {
            val descriptor = new ValueStateDescriptor[Long]("events-counter", classOf[Long])
            stateCounter = getRuntimeContext.getState(descriptor)
          }

          override def processElement(
                                       value: T,
                                       ctx: KeyedProcessFunction[String, T, T]#Context,
                                       out: Collector[T]
                                     ): Unit = {
            val currentState = Option(stateCounter.value()).getOrElse(0L)
            stateCounter.update(currentState + 1)
            out.collect(value)
          }
        }
      )

      // 3-) Instantiate Kafka Producer with Avro serialization
      val kafkaProducer = new FlinkKafkaProducer[T](
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

    private def createSerializer(): GenericAvroSerializer[T] = {
      new GenericAvroSerializer[T](
        schemaString = schemaString,
        topic = topicName,
        toGenericRecord = toGenericRecord
      )
    }
  }

  // Avro schemas
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

  // Implementation for Income Prediction Producer
  class IncomePredictionProducer extends StreamProducer[predictionRequest, predictionRequest](
    keySelector = _.customerId.getOrElse(0).toString
  )(implicitly[TypeInformation[predictionRequest]], implicitly[TypeInformation[predictionRequest]]) {
    override protected def schemaString: String = SCHEMA_STRING_INCOME_PREDICTION
    override protected def topicName: String = "income_prediction_request"
    override protected def printEnabled: Boolean = false
    override protected def convertSourceToTarget(source: predictionRequest): predictionRequest = source
    override protected def createSourceGenerator(): SourceFunction[predictionRequest] =
      new predictionRequestGenerator(sleepMillisPerEvent)

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

  // Implementation for NPL Producer
  class NplPredictionProducer extends StreamProducer[predictionRequest, predictionRequest](
    keySelector = _.customerId.getOrElse(0).toString
  )(implicitly[TypeInformation[predictionRequest]], implicitly[TypeInformation[predictionRequest]]) {
    override protected def schemaString: String = SCHEMA_STRING_NPL_PREDICTION
    override protected def topicName: String = "npl_prediction_request"
    override protected def printEnabled: Boolean = true
    override protected def convertSourceToTarget(source: predictionRequest): predictionRequest = source
    override protected def createSourceGenerator(): SourceFunction[predictionRequest] =
      new predictionRequestGenerator(sleepMillisPerEvent)

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