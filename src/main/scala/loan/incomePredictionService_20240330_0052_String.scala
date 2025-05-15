//package loan
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//
//import java.util.Properties
//
//object incomePredictionService_20240330_0052_String {
//
//  def incomePredictionRequestConsumer(): Unit = {
//
//    // 1-) Setup Environment
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    // 2-) Instantiate Kafka Consumer
//    val kafkaProps = new Properties()
//    kafkaProps.setProperty("bootstrap.servers", "localhost:9092")   // Kafka broker
//    kafkaProps.setProperty("group.id", "income_prediction_request_consumer")      // Consumer group
//
//    val kafkaConsumer = new FlinkKafkaConsumer[String](
//    "income_prediction_request",         // Kafka topic
//    new SimpleStringSchema(),            // Deserialize messages as String
//    kafkaProps
//    )
//
//    kafkaConsumer.setStartFromEarliest()  // Read from beginning if no checkpoint
//
//    // 3-) Pass Kafka Event to Stream
//    val stream = env.addSource(kafkaConsumer)     // Add Kafka source to Flink
//
//    stream                                        // Process the stream (e.g., parse JSON, filter, map, etc.)
//    .map(msg => s"Received income prediction Request: $msg")
//    .print()
//
//
//    // Execute Flink job
//    env.execute("Kafka Flink Consumer")
//
//  }
//
//  def main(args: Array[String]): Unit = {
//    incomePredictionRequestConsumer()
//  }
//}
