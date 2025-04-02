package loan

import generators.{incomePredictionRequest, incomePredictionRequestGenerator}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.ProducerRecord

import java.io.ByteArrayOutputStream
import java.util.Properties


object loanDecisionService {

  // Create Avro schema for incomePredictionRequest
  val SCHEMA_STRING =
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

  val SCHEMA = new Schema.Parser().parse(SCHEMA_STRING)

  // Custom Kafka serialization schema for Avro
  class AvroKafkaSerializationSchema extends KafkaSerializationSchema[incomePredictionRequest] {
    override def serialize(element: incomePredictionRequest, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
      val byteArrayOutputStream = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().blockingBinaryEncoder(byteArrayOutputStream, null)

      // Since incomePredictionRequest is not an Avro SpecificRecord, we use GenericDatumWriter
      val writer = new GenericDatumWriter[GenericRecord](SCHEMA)

      // Convert our case class to a GenericRecord
      val record = new GenericData.Record(SCHEMA)
      record.put("requestId", element.requestId.orNull)
      record.put("applicationId", element.applicationId.orNull)
      record.put("customerId", element.customerId.getOrElse(0))
      record.put("prospectId", element.prospectId.getOrElse(0))
      record.put("requestedAt", element.requestedAt.getOrElse(0L))
      record.put("incomeSource", element.incomeSource)
      record.put("isCustomer", element.isCustomer)

      writer.write(record, encoder)
      encoder.flush()

      val avroBytes = byteArrayOutputStream.toByteArray
      byteArrayOutputStream.close()

      // Return Kafka record with null key and Avro data as value
      new ProducerRecord[Array[Byte], Array[Byte]](
        "income_prediction_request",  // topic
        null,                         // key
        avroBytes                     // values
      )
    }
  }

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
      //incomePredictionRequestEvents.keyBy(_.customerId)

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


    // 3-) Instantiate Kafka Producer with Avro serialization
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProps.setProperty("transaction.timeout.ms", "5000")

    val kafkaProducer = new FlinkKafkaProducer[incomePredictionRequest](
      "income_prediction_request",        // default topic
      new AvroKafkaSerializationSchema(), // serialize as Avro
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