//
//package loan
//
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//
//import java.util.Properties
//
//object incomePredictionService_20240331_1427_Avro_Debug {
//
//  // Simple utility to convert bytes to hex string
//  def bytesToHex(bytes: Array[Byte]): String = {
//    val hexChars = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f')
//    val result = new StringBuilder(bytes.length * 2)
//
//    for (b <- bytes) {
//      val v = b & 0xFF
//      result.append(hexChars(v >>> 4))
//      result.append(hexChars(v & 0x0F))
//    }
//
//    result.toString
//  }
//
//  def incomePredictionRequestConsumer(): Unit = {
//    // 1-) Setup Environment
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    // 2-) Configure Kafka properties
//    val kafkaProps = new Properties()
//    kafkaProps.setProperty("bootstrap.servers", "localhost:9092")
//    kafkaProps.setProperty("group.id", "income_prediction_diagnostic")
//
//    // 3-) Use a custom schema to get raw bytes
//    class DiagnosticSchema extends SimpleStringSchema {
//      override def deserialize(message: Array[Byte]): String = {
//        try {
//          // Convert bytes to hex string for viewing
//          val hexString = bytesToHex(message)
//
//          // Try to show ASCII representation where possible
//          val printableString = new StringBuilder(message.length)
//          for (b <- message) {
//            if (b >= 32 && b <= 126) { // Printable ASCII range
//              printableString.append(b.toChar)
//            } else {
//              printableString.append('·') // Replace non-printable chars
//            }
//          }
//
//          s"RAW[${message.length}]: hex=${hexString.take(150)}${if (hexString.length > 150) "..." else ""}, " +
//            s"ascii=${printableString.toString.take(150)}${if (printableString.length > 150) "..." else ""}"
//        } catch {
//          case e: Exception =>
//            s"ERROR: ${e.getMessage} (${if (message != null) message.length else "null"} bytes)"
//        }
//      }
//    }
//
//    // 4-) Create Kafka consumer with our diagnostic schema
//    val kafkaConsumer = new FlinkKafkaConsumer[String](
//      "income_prediction_request",
//      new DiagnosticSchema(),
//      kafkaProps
//    )
//
//    kafkaConsumer.setStartFromEarliest()  // Read from beginning if no checkpoint
//
//    // 5-) Create stream and print the raw data
//    val stream = env.addSource(kafkaConsumer)
//    stream.print()
//
//    // 6-) Execute Flink job
//    env.execute("Raw Kafka Message Diagnostic")
//  }
//
//  def main(args: Array[String]): Unit = {
//    incomePredictionRequestConsumer()
//  }
//}