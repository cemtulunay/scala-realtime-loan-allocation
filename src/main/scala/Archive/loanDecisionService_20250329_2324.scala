package Archive

//package loan
//
//import org.apache.flink.streaming.api.scala.{KeyedStream, StreamExecutionEnvironment}
//import generators.{incomePredictionRequest, incomePredictionRequestGenerator}
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
//import org.apache.flink.util.Collector
//
//import java.util.Properties
//
//object loanDecisionService_20240329_2324 {
//
//  def incomePredictionRequestProducer(): Unit = {
//
//    // 1-) Setup Environment and add Data generator as a Source
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val incomePredictionRequestEvents = env.addSource(
//      new incomePredictionRequestGenerator(
//        sleepMillisPerEvent = 100, // ~ 10 events/s
//      )
//    )
//    //val eventsPerRequest: KeyedStream[incomePredictionRequest, String] = incomePredictionRequestEvents.keyBy(_.requestId.getOrElse("unknown"))
//    // val eventsPerRequest: KeyedStream[incomePredictionRequest, String] = incomePredictionRequestEvents.keyBy(_.customerId.getOrElse("0"))
//    val eventsPerRequest: KeyedStream[incomePredictionRequest, String] = incomePredictionRequestEvents.keyBy(_.customerId)
//
//    /** ****************************************************************************************************************************************************
//     *  STATEFUL APPROACH - State Primitives - ValueState - Distributed Available
//     * **************************************************************************************************************************************************** */
//
//    // 2-) Create Event Stream
//    val numEventsPerRequestStream = eventsPerRequest.process(
//      new KeyedProcessFunction[String, incomePredictionRequest, String] {
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
//                                     value: incomePredictionRequest,
//                                     ctx: KeyedProcessFunction[String, incomePredictionRequest, String]#Context,
//                                     out: Collector[String]
//                                   ): Unit = {
//
//          val currentState = Option(stateCounter.value()).getOrElse(0L) // If state is null, use 0L
//
//          // Update state with the new count
//          stateCounter.update(currentState + 1)
//
//          // Collect the output
//          // out.collect(s"request ${value.requestId.getOrElse("unknown")} - ${currentState + 1} - customer ${value.customerId.getOrElse("0")}")
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
//    // 4-) Pass Stream (That is created with Data generator) to Kafka Producer
//    numEventsPerRequestStream.addSink(kafkaProducer)
//    numEventsPerRequestStream.print()
//
//    env.execute()
//  }
//
//  def main(args: Array[String]): Unit = {
//    incomePredictionRequestProducer()
//  }
//}
//
//
