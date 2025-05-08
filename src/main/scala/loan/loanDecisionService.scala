package loan

import generators.{incomePredictionRequest, incomePredictionRequestGenerator}
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

  def incomePredictionRequestProducer(): Unit = {

    // 1-) Setup Environment and add Data generator as a Source
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val incomePredictionRequestEvents = env.addSource(
      new incomePredictionRequestGenerator(
        sleepMillisPerEvent = 100, // ~ 10 events/s
      )
    )
    val eventsPerRequest: KeyedStream[incomePredictionRequest, String] =
      incomePredictionRequestEvents.keyBy(_.customerId.getOrElse(0).toString)

    /** ****************************************************************************************************************************************************
     *  STATEFUL APPROACH - State Primitives - ValueState - Distributed Available
     * **************************************************************************************************************************************************** */

    // 2-) Create Event Stream
    val processedRequests = eventsPerRequest.process(
      new KeyedProcessFunction[String, incomePredictionRequest, incomePredictionRequest] {

        var stateCounter: ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          // initialize all state
          val descriptor = new ValueStateDescriptor[Long]("events-counter", classOf[Long])
          stateCounter = getRuntimeContext.getState(descriptor)
        }

        override def processElement(
                                     value: incomePredictionRequest,
                                     ctx: KeyedProcessFunction[String, incomePredictionRequest, incomePredictionRequest]#Context,
                                     out: Collector[incomePredictionRequest]
                                   ): Unit = {
          val currentState = Option(stateCounter.value()).getOrElse(0L) // If state is null, use 0L

          // Update state with the new count
          stateCounter.update(currentState + 1)

          // For debugging, print to stdout
          //println(s"request ${value.requestId.getOrElse("unknown")} - ${currentState + 1} - customer ${value.customerId.getOrElse("0")}")

          // Forward the original request to Kafka
          out.collect(value)
        }
      }
    )

    // Use the schema string instead of Schema object
    val incomePredictionSerializer = new GenericAvroSerializer[incomePredictionRequest](
      schemaString = SCHEMA_STRING_INCOME_PREDICTION,  // Use the schema string constant
      topic = "income_prediction_request",
      toGenericRecord = (element, record) => {
        record.put("requestId", element.requestId.orNull)
        record.put("applicationId", element.applicationId.orNull)
        record.put("customerId", element.customerId.getOrElse(0))
        record.put("prospectId", element.prospectId.getOrElse(0))
        record.put("requestedAt", element.requestedAt.getOrElse(0L))
        record.put("incomeSource", element.incomeSource)
        record.put("isCustomer", element.isCustomer)
      }
    )

    // 3-) Instantiate Kafka Producer with Avro serialization
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProps.setProperty("transaction.timeout.ms", "5000")

    val kafkaProducer = new FlinkKafkaProducer[incomePredictionRequest](
      "income_prediction_request",        // default topic
      incomePredictionSerializer,         // serialize as Avro
      kafkaProps,
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE
    )

    // 4-) Pass Stream to Kafka Producer
    processedRequests.addSink(kafkaProducer)
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    incomePredictionRequestProducer()
  }
}