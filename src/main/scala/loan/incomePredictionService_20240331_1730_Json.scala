package loan

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.json4s.jackson.JsonMethods
import org.json4s.DefaultFormats

import java.util.Properties

object incomePredictionService_20240331_1730_Json {

  // JSON parsing formats
  implicit val formats = DefaultFormats

  // Custom deserializer that handles null values
  class RobustStringSchema extends AbstractDeserializationSchema[String] {
    override def deserialize(message: Array[Byte]): String = {
      if (message == null) {
        return "[null message]" // Return a placeholder for null messages
      }
      try {
        new String(message, "UTF-8")
      } catch {
        case e: Exception =>
          // Handle any deserialization errors
          s"[Error: ${e.getMessage}]"
      }
    }
  }

  def incomePredictionRequestConsumer(): Unit = {
    // 1-) Setup Environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2-) Instantiate Kafka Consumer with robust string schema
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProps.setProperty("group.id", "income_prediction_request_consumer")

    // Use our custom robust deserializer instead of SimpleStringSchema
    val kafkaConsumer = new FlinkKafkaConsumer[String](
      "income_prediction_request",
      new RobustStringSchema(),
      kafkaProps
    )

    kafkaConsumer.setStartFromEarliest()

    // 3-) Pass Kafka Event to Stream
    val stream = env.addSource(kafkaConsumer)

    // 4-) Process the stream with JSON parsing and error handling
    stream
      .map(jsonString => {
        if (jsonString == "[null message]") {
          "Received null message from Kafka"
        } else if (jsonString.startsWith("[Error:")) {
          s"Deserialization error: $jsonString"
        } else {
          try {
            // Only attempt to parse valid JSON strings
            val json = JsonMethods.parse(jsonString)

            // Extract fields with null-safety
            val requestId = try { (json \ "requestId").extract[String] } catch { case _: Exception => null }
            val customerId = try { (json \ "customerId").extract[String] } catch { case _: Exception => null }
            val prospectId = try { (json \ "prospectId").extract[String] } catch { case _: Exception => null }
            val incomeSource = try { (json \ "incomeSource").extract[String] } catch { case _: Exception => "unknown" }
            val isCustomer = try { (json \ "isCustomer").extract[Boolean] } catch { case _: Exception => false }
            val eventCount = try { (json \ "eventCount").extract[Long] } catch { case _: Exception => 0L }

            s"Processed JSON Request: ID=${Option(requestId).getOrElse("unknown")}, " +
              s"Customer=${Option(customerId).getOrElse("N/A")}, " +
              s"Prospect=${Option(prospectId).getOrElse("N/A")}, " +
              s"Source=$incomeSource, " +
              s"Customer=$isCustomer, " +
              s"Count=$eventCount"
          } catch {
            case e: Exception =>
              // Print the first part of the message for debugging
              val preview = if (jsonString.length > 100) jsonString.take(100) + "..." else jsonString
              s"Error parsing JSON: ${e.getMessage}, Raw data: $preview"
          }
        }
      })
      .print()

    // Execute Flink job
    env.execute("Robust JSON Kafka Consumer")
  }

  def main(args: Array[String]): Unit = {
    incomePredictionRequestConsumer()
  }
}