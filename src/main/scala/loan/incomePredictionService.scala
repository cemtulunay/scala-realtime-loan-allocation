package loan


import generators.{incomePredictionRequest, incomePredictionRequestGenerator}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.util.Collector

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import java.util.Properties

object incomePredictionService {

  def incomePredictionRequestConsumer(): Unit = {

    // 1-) Setup Environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Kafka properties
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "localhost:9092")   // Kafka broker
    kafkaProps.setProperty("group.id", "income_prediction_request_consumer")      // Consumer group

    // Create Kafka consumer
    val kafkaConsumer = new FlinkKafkaConsumer[String](
    "income_prediction_request",         // Kafka topic
    new SimpleStringSchema(),            // Deserialize messages as String
    kafkaProps
    )

    kafkaConsumer.setStartFromEarliest()  // Read from beginning if no checkpoint

    // Add Kafka source to Flink
    val stream = env.addSource(kafkaConsumer)

    // Process the stream (e.g., parse JSON, filter, map, etc.)
    stream
    .map(msg => s"Received income prediction Request: $msg")
    .print()

    // Execute Flink job
    env.execute("Kafka Flink Consumer")

  }

  def main(args: Array[String]): Unit = {
    incomePredictionRequestConsumer()
  }
}
