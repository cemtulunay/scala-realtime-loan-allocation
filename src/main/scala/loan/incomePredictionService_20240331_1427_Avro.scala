//package loan
//
//import org.apache.avro.Schema
//import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
//import org.apache.avro.io.{DatumReader, DecoderFactory}
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
//import org.apache.kafka.clients.consumer.ConsumerRecord
//
//import java.io.ByteArrayInputStream
//import java.util.Properties
//
//object incomePredictionService_20240331_1427_Avro {
//
//  // Custom Kafka Avro Deserialization Schema
//  class AvroDeserializationSchema(schema: Schema) extends KafkaDeserializationSchema[GenericRecord] {
//    @transient private lazy val reader: DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord](schema)
//
//    override def isEndOfStream(nextElement: GenericRecord): Boolean = false
//
//    override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): GenericRecord = {
//      val bytes = record.value()
//
//      // Skip the first byte (header)
//      val avroBytes = bytes.slice(1, bytes.length)
//
//      val decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(avroBytes), null)
//      reader.read(null, decoder)
//    }
//
//    override def getProducedType(): TypeInformation[GenericRecord] = {
//      implicit val typeInfo: TypeInformation[GenericRecord] =
//        TypeInformation.of(classOf[GenericRecord])
//      typeInfo
//    }
//  }
//
//  def incomePredictionRequestConsumer(): Unit = {
//
//    // 1-) Setup Environment
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    // 2-) Define Avro schema (either from a file or as a string)
//    // Option 1: Load schema from file
//    //    val schemaFile = new File("path/to/income_prediction_request.avsc")
//    //    val schema = new Schema.Parser().parse(schemaFile)
//
//    // Option 2: Define schema inline (uncomment if preferred)
//    val schemaString =
//      """
//        |{
//        |  "type": "record",
//        |  "name": "IncomePredictionRequest",
//        |  "namespace": "loan",
//        |  "fields": [
//        |    {"name": "requestId", "type": ["string"]},
//        |    {"name": "applicationId", "type": ["string"]},
//        |    {"name": "customerId", "type": ["int"]},
//        |    {"name": "prospectId", "type": ["int"]},
//        |    {"name": "requestedAt", "type": ["long"]},
//        |    {"name": "incomeSource", "type": ["null","string"], "default": null},
//        |    {"name": "isCustomer", "type": ["null","boolean"], "default": null}
//        |  ]
//        |}
//    """.stripMargin
//    val schema = new Schema.Parser().parse(schemaString)
//
//    // 3-) Configure Kafka properties
//    val kafkaProps = new Properties()
//    kafkaProps.setProperty("bootstrap.servers", "localhost:9092")
//    kafkaProps.setProperty("group.id", "income_prediction_request_consumer")
//
//    // For Confluent Schema Registry (if using)
//    // kafkaProps.setProperty("schema.registry.url", "http://localhost:8081")
//
//    // 4-) Create custom Avro deserialization schema
//    val avroDeserializer = new AvroDeserializationSchema(schema)
//
//    // 5-) Create Kafka consumer with Avro deserializer
//    val kafkaConsumer = new FlinkKafkaConsumer[GenericRecord](
//      "income_prediction_request",
//      avroDeserializer,
//      kafkaProps
//    )
//
//    kafkaConsumer.setStartFromEarliest()  // Read from beginning if no checkpoint
//
//    // 6-) Add source to Flink environment and process
//    val stream = env.addSource(kafkaConsumer)
//
//    // 7-) Process the Avro records
//    stream.map(record => {
//        try {
//          val requestId = record.get("requestId").toString
//          val applicationId = record.get("applicationId").toString
//          val customerId = record.get("customerId").toString
//          val prospectId = record.get("prospectId").toString
//          val requestedAt = record.get("requestedAt").toString
//          val incomeSource = record.get("incomeSource").toString
//          val isCustomer = record.get("isCustomer").toString
//          s"requestId: $requestId, applicationId: $applicationId, customerId: $customerId, prospectId: $prospectId, requestedAt: $requestedAt, incomeSource: $incomeSource, isCustomer: $isCustomer"
//        } catch {
//          case e: Exception =>
//            println(s"Error processing record: ${e.getMessage}")
//            e.printStackTrace()
//            "Error"
//        }
//      })
//      .print()
//
//    // 8-) Execute Flink job
//    env.execute()
//  }
//
//  def main(args: Array[String]): Unit = {
//    incomePredictionRequestConsumer()
//  }
//}
