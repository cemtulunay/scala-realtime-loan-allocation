//package target
//
//import org.apache.avro.Schema
//import org.apache.avro.generic.GenericRecord
//import org.apache.flink.api.common.functions.MapFunction
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.api.java.typeutils.TypeExtractor
//import org.apache.flink.streaming.api.datastream.DataStream
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import serializer.{GenericAvroDeserializer, GenericRecordKryoSerializer}
//
//import java.lang.reflect.ParameterizedType
//import scala.reflect.ClassTag
//
//// InfluxDB imports
//import com.influxdb.client.{InfluxDBClient, InfluxDBClientFactory, WriteApi}
//import com.influxdb.client.write.Point
//import com.influxdb.client.domain.WritePrecision
//
//import java.util.Properties
//import java.time.Instant
//
///**
// * A generalized abstract class for Flink-based Kafka consumers that read data from a Kafka topic
// * and save it to InfluxDB for real-time analytics. This class handles the common pipeline setup
// * for consuming, processing, and storing time-series data.
// *
// * @param topicName Name of the Kafka topic to consume from
// * @param schemaString Avro schema for deserializing Kafka messages
// * @param bootstrapServers Kafka server addresses (comma-separated)
// * @param consumerGroupId Identifier for the consumer group
// * @param influxUrl InfluxDB connection URL
// * @param influxToken Authentication token for InfluxDB
// * @param influxOrg Organization name in InfluxDB
// * @param influxBucket Bucket name in InfluxDB
// * @param measurement InfluxDB measurement name (equivalent to table)
// * @param batchSize Number of points to batch before writing to InfluxDB
// * @param flushIntervalMs Maximum time (ms) to wait before flushing batch
// * @param checkpointingIntervalMs Interval (ms) between state checkpoints
// * @param autoOffsetReset Strategy for offset when none exists (latest/earliest)
// * @param enableAutoCommit Whether to auto-commit offsets to Kafka
// * @tparam T The target domain object type (must be Serializable)
// */
//abstract class AnalyticalStreamConsumer[T <: Serializable](
//                                                            topicName: String,
//                                                            schemaString: String,
//                                                            bootstrapServers: String = "localhost:9092",
//                                                            consumerGroupId: String,
//                                                            influxUrl: String = "http://localhost:8086",
//                                                            influxToken: String = "loan-analytics-token",
//                                                            influxOrg: String = "loan-org",
//                                                            influxBucket: String = "loan-analytics",
//                                                            measurement: String,
//                                                            batchSize: Int = 1000,
//                                                            flushIntervalMs: Int = 1000,
//                                                            checkpointingIntervalMs: Int = 10000,
//                                                            autoOffsetReset: String = "earliest",
//                                                            enableAutoCommit: String = "false",
//                                                            printToConsole: Boolean = false
//                                                          ) {
//
//  protected val schema: Schema = new Schema.Parser().parse(schemaString)
//
//  // 1-) Configure Kafka properties
//  protected def createKafkaProperties(): Properties = {
//    val kafkaProps = new Properties()
//    kafkaProps.setProperty("bootstrap.servers", bootstrapServers)
//    kafkaProps.setProperty("group.id", consumerGroupId)
//    kafkaProps.setProperty("auto.offset.reset", autoOffsetReset)
//    kafkaProps.setProperty("enable.auto.commit", enableAutoCommit)
//    kafkaProps
//  }
//
//  // 2-) Create a Kafka consumer with generic Avro deserializer
//  //  protected def createKafkaConsumer(): FlinkKafkaConsumer[GenericRecord] = {
//  //
//  //    println(s"CRITICAL_createKafkaConsumer: Creating Kafka consumer for topic: $topicName")
//  //    println(s"CRITICAL_createKafkaConsumer: Using schema: ${schema.toString(true)}")
//  //    println(s"CRITICAL_createKafkaConsumer: Kafka properties: bootstrap.servers=${bootstrapServers}, group.id=${consumerGroupId}, auto.offset.reset=${autoOffsetReset}")
//  //
//  //    val consumer = new FlinkKafkaConsumer[GenericRecord](
//  //      topicName,
//  //      new GenericAvroDeserializer(schema),
//  //      createKafkaProperties()
//  //    )
//  //
//  //    if (autoOffsetReset == "earliest") {
//  //      println("CRITICAL_createKafkaConsumer: Setting consumer to start from earliest offset")
//  //      consumer.setStartFromEarliest()
//  //    } else {
//  //      println("CRITICAL_createKafkaConsumer: Setting consumer to start from latest offset")
//  //      consumer.setStartFromLatest()
//  //    }
//  //
//  //    println(s"CRITICAL_createKafkaConsumer: Kafka consumer created successfully for topic: $topicName")
//  //    consumer
//  //  }
//
//  // In AnalyticalStreamConsumer.scala
//  protected def createKafkaConsumer(): FlinkKafkaConsumer[Array[Byte]] = {
//    println(s"Creating Kafka consumer for topic: $topicName with custom ByteArray deserializer")
//
//    val consumer = new FlinkKafkaConsumer[Array[Byte]](
//      topicName,
//      new CustomByteArrayDeserializer(), // Define this class
//      createKafkaProperties()
//    )
//
//    if (autoOffsetReset == "earliest") {
//      consumer.setStartFromEarliest()
//    } else {
//      consumer.setStartFromLatest()
//    }
//
//    println(s"Kafka consumer created successfully")
//    consumer
//  }
//
//  // Define this helper class
//  class CustomByteArrayDeserializer extends KafkaDeserializationSchema[Array[Byte]] {
//    override def isEndOfStream(nextElement: Array[Byte]): Boolean = false
//
//    override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): Array[Byte] = {
//      println(s"Deserializing record from partition=${record.partition()}, offset=${record.offset()}")
//      record.value()
//    }
//
//    override def getProducedType(): TypeInformation[Array[Byte]] =
//      TypeInformation.of(classOf[Array[Byte]])
//  }
//
//  // 3-) Map GenericRecord to domain object
//  //protected def createRecordMapper(): MapFunction[GenericRecord, T]
//  protected def createRecordMapper(): MapFunction[Array[Byte], T]
//
//  // 4-) Convert domain object to InfluxDB Point (with tags and fields)
//  protected def convertToInfluxPoint(record: T): Point
//
//  // 5-) Create the main data stream - this is what was missing in your analytics service!
//  protected def createMainDataStream(env: StreamExecutionEnvironment): DataStream[T] = {
//    System.out.println("CRITICAL_createMainDataStream: createMainDataStream method CALLED")
//    println(s"CRITICAL_createMainDataStream: Creating main data stream for topic: $topicName")
//
//    //    val tClass = this.getClass.getSuperclass
//    //      .getGenericSuperclass
//    //      .asInstanceOf[ParameterizedType]
//    //      .getActualTypeArguments()(0)
//    //      .asInstanceOf[Class[T]]
//
//    //implicit val typeInfo: TypeInformation[T] = TypeInformation.of(tClass)
//
//    //    implicit val typeInfo: TypeInformation[T] = TypeExtractor.createTypeInfo(
//    //      this.getClass.getSuperclass.getGenericSuperclass.asInstanceOf[ParameterizedType].getActualTypeArguments()(0)
//    //    )
//
//    //    implicit val typeInfo: TypeInformation[T] = TypeInformation.of(
//    //      this.getClass.getSuperclass.getGenericSuperclass.asInstanceOf[ParameterizedType]
//    //        .getActualTypeArguments()(0).asInstanceOf[Class[T]]
//    //    )
//
//    //implicit val typeInfo: TypeInformation[T] = createTypeInformation[T]
//
//    val scalaEnv = StreamExecutionEnvironment.getExecutionEnvironment
//
//    // Get the raw source first
//    val rawSource = env.addSource(createKafkaConsumer())
//
//    System.out.println("CRITICAL_createMainDataStream: Raw source created")
//
//    // Add a print to see raw records BEFORE mapping
//    //    if (printToConsole) {
//    println("CRITICAL_createMainDataStream: Adding print operator for raw Kafka records")
//    rawSource.print("CRITICAL_createMainDataStream: RAW_KAFKA_RECORD")
//    //    }
//
//    // Now do the mapping
//    //    val stream = rawSource
//    //      .map(record => {
//    //        System.out.println(s"CRITICAL: Mapping record: ${record}")
//    //        println(s"DIAGNOSTIC101: Received record from Kafka: ${record}")
//    //        try {
//    //          val mappedRecord = createRecordMapper().map(record)
//    //          println(s"DIAGNOSTIC102: Mapped to domain object: ${mappedRecord}")
//    //          mappedRecord
//    //        } catch {
//    //          case e: Exception =>
//    //            println(s"ERROR mapping record: ${e.getMessage}")
//    //            println(s"Problematic record: ${record}")
//    //            System.out.println(s"CRITICAL ERROR mapping record: ${e.getMessage}")
//    //            e.printStackTrace()
//    //            throw e
//    //        }
//    //      })
//    //      .name(s"${topicName}_analytical_stream")
//
//
//    val stream: DataStream[T] = rawSource
//      .map { record =>
//        System.out.println(s"CRITICAL_createMainDataStream: Mapping record: ${record}")
//
//        try {
//          val mappedRecord = createRecordMapper().map(record)
//          System.out.println(s"CRITICAL_createMainDataStream: Mapped record: ${mappedRecord}")
//          mappedRecord
//        } catch {
//          case e: Exception =>
//            System.out.println(s"CRITICAL_createMainDataStream: ERROR mapping record: ${e.getMessage}")
//            e.printStackTrace()
//            throw e
//        }
//      }
//      .name(s"CRITICAL_createMainDataStream: ${topicName}_analytical_stream")
//
//    //    val stream = rawSource
//    //      .map(new MapFunction[GenericRecord, T] {
//    //        override def map(record: GenericRecord): T = {
//    //          // Use System.out.println to ensure visibility
//    //          System.out.println(s"CRITICAL: Mapping record: ${record}")
//    //
//    //          try {
//    //            val mappedRecord = createRecordMapper().map(record)
//    //            System.out.println(s"CRITICAL: Mapped record: ${mappedRecord}")
//    //            mappedRecord
//    //          } catch {
//    //            case e: Exception =>
//    //              System.out.println(s"CRITICAL ERROR mapping record: ${e.getMessage}")
//    //              e.printStackTrace()
//    //              throw e
//    //          }
//    //        }
//    //      })
//    //      .name(s"${topicName}_analytical_stream")
//
//    //println("Main data stream created successfully")
//    System.out.println("CRITICAL_createMainDataStream: Stream created", stream)
//    stream
//
//
//    //    println(s"Creating main data stream for topic: $topicName")
//    //    val stream = env.addSource(createKafkaConsumer())
//    //      .map(record => {
//    //        println(s"Received record from Kafka: ${record}")
//    //        val mappedRecord = createRecordMapper().map(record)
//    //        println(s"Mapped to domain object: ${mappedRecord}")
//    //        mappedRecord
//    //      })
//    //      .name(s"${topicName}_analytical_stream")
//    //
//    //    println("Main data stream created successfully")
//    //    stream
//
//    //    env.addSource(createKafkaConsumer())
//    //      .map(createRecordMapper())
//    //      .name(s"${topicName}_analytical_stream")
//  }
//
//  // 6-) Override this for custom stream processing (optional)
//  protected def createCustomDataStream(env: StreamExecutionEnvironment): Unit = {
//    //    // Default implementation just writes to InfluxDB
//    //    val stream = createMainDataStream(env)
//    //
//    //    if (printToConsole) {
//    //      stream.print()
//    //    }
//    //
//    //    stream.addSink(new InfluxDBSink())
//    // Default implementation just writes to InfluxDB
//    println("CRITICAL_createCustomDataStream: Using default stream processing implementation")
//    val stream = createMainDataStream(env)
//    println(s"JAVA STREAM CREATED: $stream")
//
//
//    //if (printToConsole) {
//    println("CRITICAL_createCustomDataStream: Adding print operator to see data on console")
//    stream.print("CRITICAL_createCustomDataStream: RAW_RECORD")
//    //}
//
//    println("CRITICAL_createCustomDataStream: Adding InfluxDB sink to stream")
//    stream.addSink(new InfluxDBSink())
//    println("CRITICAL_createCustomDataStream: InfluxDB sink added successfully")
//  }
//
//  // 7-) InfluxDB Sink Implementation
//  class InfluxDBSink extends RichSinkFunction[T] {
//    //    private var influxClient: InfluxDBClient = _
//    //    private var writeApi: WriteApi = _
//    //
//    //    override def open(parameters: Configuration): Unit = {
//    //      super.open(parameters)
//    //      influxClient = InfluxDBClientFactory.create(influxUrl, influxToken.toCharArray, influxOrg, influxBucket)
//    //      writeApi = influxClient.getWriteApi
//    //    }
//    //
//    //    override def invoke(record: T, context: org.apache.flink.streaming.api.functions.sink.SinkFunction.Context): Unit = {
//    //      try {
//    //        val point = convertToInfluxPoint(record)
//    //        writeApi.writePoint(point)
//    //      } catch {
//    //        case e: Exception =>
//    //          println(s"Error writing to InfluxDB: ${e.getMessage}")
//    //          e.printStackTrace()
//    //          throw e
//    //      }
//    //    }
//    //
//    //    override def close(): Unit = {
//    //      if (writeApi != null) {
//    //        writeApi.close()
//    //      }
//    //      if (influxClient != null) {
//    //        influxClient.close()
//    //      }
//    //      super.close()
//    //    }
//    private var influxClient: InfluxDBClient = _
//    private var writeApi: WriteApi = _
//    private var recordCount: Int = 0
//
//    override def open(parameters: Configuration): Unit = {
//      super.open(parameters)
//      println(s"DIAGNOSTIC_InfluxDBSink_invoke: Opening InfluxDB connection to: $influxUrl")
//      println(s"DIAGNOSTIC_InfluxDBSink_invoke: InfluxDB config: org=$influxOrg, bucket=$influxBucket, measurement=$measurement")
//      influxClient = InfluxDBClientFactory.create(influxUrl, influxToken.toCharArray, influxOrg, influxBucket)
//      writeApi = influxClient.getWriteApi
//      println("DIAGNOSTIC_InfluxDBSink_invoke: InfluxDB connection opened successfully")
//    }
//
//    override def invoke(record: T, context: org.apache.flink.streaming.api.functions.sink.SinkFunction.Context): Unit = {
//      try {
//        println(s"Converting record to InfluxDB point: $record")
//        val point = convertToInfluxPoint(record)
//        println(s"DIAGNOSTIC_InfluxDBSink_invoke: Converted point: $point")
//        //println(s"Writing point to InfluxDB: measurement=${point.getMeasurement()}, tags=${point.getTags()}")
//        writeApi.writePoint(point)
//        println("DIAGNOSTIC_InfluxDBSink_invoke: Point written successfully")
//        recordCount += 1
//
//        if (recordCount % 10 == 0) {
//          println(s"DIAGNOSTIC_InfluxDBSink_invoke: Successfully wrote $recordCount records to InfluxDB")
//        }
//      } catch {
//        case e: Exception =>
//          println(s"DIAGNOSTIC_InfluxDBSink_invoke: ERROR writing to InfluxDB: ${e.getMessage}")
//          println(s"DIAGNOSTIC_InfluxDBSink_invoke: Record that failed: $record")
//          e.printStackTrace()
//          throw e
//      }
//      //      try {
//      //        println(s"About to convert record to InfluxDB point: $record")
//      //        val point = convertToInfluxPoint(record)
//      //        println(s"InfluxDB point created: measurement=${point.getMeasurement()}")
//      //
//      //        // Get some details about the point for debugging
//      //        val tags = new StringBuilder()
//      //        point.getTags().forEach(tag => {
//      //          tags.append(s"${tag.getKey()}=${tag.getValue()}, ")
//      //        })
//      //
//      //        val fields = new StringBuilder()
//      //        point.getFields().forEach(field => {
//      //          fields.append(s"${field.getKey()}=${field.getValue()}, ")
//      //        })
//      //
//      //        println(s"Point details: tags=[${tags.toString()}], fields=[${fields.toString()}]")
//      //        println("Writing point to InfluxDB...")
//      //        writeApi.writePoint(point)
//      //        println("Successfully wrote point to InfluxDB")
//      //      } catch {
//      //        case e: Exception =>
//      //          println(s"ERROR writing to InfluxDB: ${e.getMessage}")
//      //          e.printStackTrace()
//      //      }
//    }
//
//    override def close(): Unit = {
//      println(s"DIAGNOSTIC_InfluxDBSink_close: Closing InfluxDB sink. Total records written: $recordCount")
//      if (writeApi != null) {
//        writeApi.close()
//      }
//      if (influxClient != null) {
//        influxClient.close()
//      }
//      super.close()
//    }
//
//  }
//
//  // 8-) Execute the stream processing job
//  def execute(): Unit = {
//    //    // 8.1-) Setup Flink environment
//    //    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    //
//    //    // Enable checkpointing for reliability
//    //    env.enableCheckpointing(checkpointingIntervalMs)
//    //
//    //    // Register custom Kryo serializer
//    //    env.getConfig.addDefaultKryoSerializer(
//    //      classOf[GenericRecord],
//    //      classOf[GenericRecordKryoSerializer]
//    //    )
//    //
//    //    // 8.2-) Create and execute custom data stream processing
//    //    createCustomDataStream(env)
//    //
//    //    // 8.3-) Execute job
//    //    env.execute(s"Analytical Stream Consumer for $topicName")
//    println(s"=== Starting execution of Analytical Stream Consumer for topic: $topicName ===")
//    println(s"Kafka bootstrap servers: $bootstrapServers")
//    println(s"InfluxDB URL: $influxUrl")
//
//    // 8.1-) Setup Flink environment
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    println(s"Flink parallelism: ${env.getParallelism}")
//
//    // Enable checkpointing for reliability
//    env.enableCheckpointing(checkpointingIntervalMs)
//    println(s"Checkpointing enabled with interval: $checkpointingIntervalMs ms")
//
//    // Register custom Kryo serializer
//    env.getConfig.addDefaultKryoSerializer(
//      classOf[GenericRecord],
//      classOf[GenericRecordKryoSerializer]
//    )
//    println("Registered custom Kryo serializer for GenericRecord")
//
//    // 8.2-) Create and execute custom data stream processing
//    println("Creating custom data stream processing...")
//    createCustomDataStream(env)
//    println("Custom data stream created, ready to execute")
//
//    // 8.3-) Execute job
//    println(s"Executing Flink job: Analytical Stream Consumer for $topicName")
//    env.execute(s"Analytical Stream Consumer for $topicName")
//  }
//
//  def testInfluxDBConnection(): Unit = {
//    println("Testing InfluxDB connection...")
//    try {
//      val influxDB = InfluxDBClientFactory.create(
//        influxUrl,
//        influxToken.toCharArray,
//        influxOrg,
//        influxBucket
//      )
//
//      val health = influxDB.health()
//      println(s"InfluxDB connection test result: ${health.getStatus()}")
//      println(s"InfluxDB version: ${health.getVersion()}")
//
//      // Write a test point to verify write access
//      val writeApi = influxDB.getWriteApi
//      val testPoint = Point.measurement(measurement)
//        .addTag("test_type", "startup_test")
//        .addField("value", 1.0)
//        .time(Instant.now(), WritePrecision.MS)
//
//      println(s"Writing test point to measurement: $measurement")
//      writeApi.writePoint(testPoint)
//      println("Test point written successfully")
//
//      writeApi.close()
//      influxDB.close()
//      println("InfluxDB connection test completed successfully")
//    } catch {
//      case e: Exception =>
//        println(s"ERROR testing InfluxDB connection: ${e.getMessage}")
//        e.printStackTrace()
//    }
//  }
//
//  def testDirectWrite(): Unit = {
//    println("Testing direct write to InfluxDB...")
//    try {
//      val influxDB = InfluxDBClientFactory.create(
//        influxUrl,
//        influxToken.toCharArray,
//        influxOrg,
//        influxBucket
//      )
//
//      val writeApi = influxDB.getWriteApi
//      val point = Point.measurement(measurement)
//        .addTag("test", "direct_write")
//        .addTag("loan_decision", "true")
//        .addField("loan_amount", 15000.0)
//        .addField("risk_score", 0.45)
//        .time(Instant.now(), WritePrecision.MS)
//
//      println(s"Writing test point directly to measurement: $measurement")
//      writeApi.writePoint(point)
//      println("Direct write test completed successfully")
//
//      writeApi.close()
//      influxDB.close()
//    } catch {
//      case e: Exception =>
//        println(s"ERROR in direct write test: ${e.getMessage}")
//        e.printStackTrace()
//    }
//  }
//
//  def analyzeBinaryFormat(bytes: Array[Byte]): Unit = {
//    println("Detailed binary format analysis:")
//    println(s"Total length: ${bytes.length} bytes")
//
//    // Assuming the format is:
//    // 'H' + UUID + 'H' + UUID + binary data
//    if (bytes.length >= 74) { // 1 + 36 + 1 + 36 = 74
//      val requestId = new String(bytes, 1, 36)
//      val applicationId = new String(bytes, 38, 36)
//
//      println(s"RequestId: $requestId")
//      println(s"ApplicationId: $applicationId")
//      println(s"Binary data starts at offset 74, length: ${bytes.length - 74} bytes")
//    } else {
//      println(s"Bytes array too short: ${bytes.length} bytes")
//    }
//  }
//
//}
//
//
//
//
//// Companion object with utility methods
//object AnalyticalStreamConsumer {
//
//  // Helper method to create common tags for InfluxDB points
//  def addCommonTags(point: Point, customerId: Int, applicationId: String): Point = {
//    point
//      .addTag("customer_id", customerId.toString)
//      .addTag("application_id", applicationId)
//      .addTag("environment", "development") // or get from config
//  }
//
//  // Helper method to extract timestamp from various sources
//  def extractTimestamp(systemTime: Option[Long], eventTime: Option[Long]): Instant = {
//    val timestamp = eventTime.orElse(systemTime).getOrElse(System.currentTimeMillis())
//    Instant.ofEpochMilli(timestamp)
//  }
//
//  // Helper method for time-based field calculations
//  def calculateElapsedTime(startTime: Long, endTime: Long = System.currentTimeMillis()): Long = {
//    endTime - startTime
//  }
//}