package Archive

//package loan
//
//import generators.{predictionRequest, predictionRequestGenerator}
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
//import org.apache.flink.util.Collector
//import org.json4s.jackson.Serialization
//import org.json4s.{DefaultFormats, Formats}
//
//import java.time.Instant
//import java.util.Properties
//
//object loanDecisionService_20240331_1730_Json {
//
//  // JSON serialization implicit formats
//  implicit val formats: Formats = DefaultFormats
//
//  // Define the case class at the object level, not inside the method
//  case class RequestData(
//                          requestId: Option[String],
//                          applicationId: Option[String],
//                          customerId: Option[Int],
//                          prospectId: Option[Int],
//                          requestedAt: Option[Long],
//                          incomeSource: String,          // "real-time" for prospects, "batch" for customers
//                          isCustomer: Boolean,
//                          eventCount: Long
//                        )
//
//  def incomePredictionRequestProducer(): Unit = {
//
//    // 1-) Setup Environment and add Data generator as a Source
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val incomePredictionRequestEvents = env.addSource(
//      new predictionRequestGenerator(
//        sleepMillisPerEvent = 100, // ~ 10 events/s
//      )
//    )
//    val eventsPerRequest: KeyedStream[predictionRequest, String] = incomePredictionRequestEvents.keyBy(_.customerId.getOrElse(0).toString)
//
//    // 2-) Create Event Stream with JSON conversion
//    val jsonStream = eventsPerRequest.process(
//      new KeyedProcessFunction[String, predictionRequest, String] {
//
//        var stateCounter: ValueState[Long] = _
//
//        override def open(parameters: Configuration): Unit = {
//          // initialize all state
//          val descriptor = new ValueStateDescriptor[Long]("events-counter", classOf[Long])
//          stateCounter = getRuntimeContext.getState(descriptor)
//        }
//
//        override def processElement(
//                                     value: predictionRequest,
//                                     ctx: KeyedProcessFunction[String, predictionRequest, String]#Context,
//                                     out: Collector[String]
//                                   ): Unit = {
//
//          val currentState = Option(stateCounter.value()).getOrElse(0L) // If state is null, use 0L
//
//          // Update state with the new count
//          stateCounter.update(currentState + 1)
//
//          // Create the output data using the case class defined at object level
//          val outputData = RequestData(
//            requestId = value.requestId,
//            applicationId = value.applicationId,
//            customerId = value.customerId,
//            prospectId = value.prospectId,
//            requestedAt = value.requestedAt,
//            incomeSource = value.incomeSource,
//            isCustomer = value.isCustomer,
//            eventCount = currentState + 1
//          )
//
//          // Convert to JSON
//          val jsonString = Serialization.write(outputData)
//
//          // Debug output
//          println(s"Sending JSON: $jsonString")
//
//          // Collect the JSON string
//          out.collect(jsonString)
//        }
//      }
//    )
//
//    // 3-) Instantiate Kafka Producer
//    val kafkaProps = new Properties()
//    kafkaProps.setProperty("bootstrap.servers", "localhost:9092") // Kafka broker
//
//    val kafkaProducer = new FlinkKafkaProducer[String](
//      "income_prediction_request",         // Kafka topic
//      new SimpleStringSchema(),            // Serialize data as String
//      kafkaProps
//    )
//
//    // 4-) Pass Stream with JSON data to Kafka Producer
//    jsonStream.addSink(kafkaProducer)
//
//    // Also print to console for debugging
//    jsonStream.print()
//
//    env.execute("JSON Kafka Producer")
//  }
//
//  def main(args: Array[String]): Unit = {
//    incomePredictionRequestProducer()
//  }
//}