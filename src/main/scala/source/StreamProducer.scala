package source

import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.util.Collector
import serializer.GenericAvroSerializer

import java.util.Properties

// Fully generalized StreamProducer
abstract class StreamProducer[S, T](
                                     protected val sleepMillisPerEvent: Int = 100,
                                     kafkaBootstrapServers: String = "localhost:9092",
                                     kafkaTransactionTimeout: Int = 5000,
                                     keySelector: T => String,
                                     flinkSemantic: FlinkKafkaProducer.Semantic = FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                                   )(implicit typeInfoS: TypeInformation[S], typeInfoT: TypeInformation[T]) extends Serializable {

  // Abstract members
  protected def schemaString: String
  protected def topicName: String
  protected def printEnabled: Boolean = false
  protected def toGenericRecord(element: T, record: GenericRecord): Unit
  protected def convertSourceToTarget(source: S): T
  protected def createSourceGenerator(): SourceFunction[S]

  // Create components in produce() method
  def produce(): Unit = {
    // 1-) Setup Environment and add Data generator as a Source
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val sourceGenerator = createSourceGenerator()

    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", kafkaBootstrapServers)
    kafkaProps.setProperty("transaction.timeout.ms", kafkaTransactionTimeout.toString)

    val serializer = createSerializer()

    // Convert from source type S to target type T
    val sourceEvents: DataStream[T] = env.addSource(sourceGenerator)
      .map(x => convertSourceToTarget(x))

    val eventsPerRequest = sourceEvents.keyBy(keySelector)

    // 2-) Create Event Stream with stateful processing
    /** ****************************************************************************************************************************************************
     *  STATEFUL APPROACH - State Primitives - ValueState - Distributed Available - Not Necessary, used for showcasing
     * **************************************************************************************************************************************************** */
    val processedRequests = eventsPerRequest.process(
      new KeyedProcessFunction[String, T, T] {
        @transient private var stateCounter: ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          val descriptor = new ValueStateDescriptor[Long]("events-counter", classOf[Long])
          stateCounter = getRuntimeContext.getState(descriptor)
        }

        override def processElement(
                                     value: T,
                                     ctx: KeyedProcessFunction[String, T, T]#Context,
                                     out: Collector[T]
                                   ): Unit = {
          val currentState = Option(stateCounter.value()).getOrElse(0L)
          stateCounter.update(currentState + 1)
          out.collect(value)
        }
      }
    )

    // 3-) Instantiate Kafka Producer with Avro serialization
    val kafkaProducer = new FlinkKafkaProducer[T](
      topicName,
      serializer,
      kafkaProps,
      flinkSemantic
    )

    // 4-) Pass Stream to Kafka Producer and print to console if required
    if (printEnabled) {
      processedRequests.print()
    }
    processedRequests.addSink(kafkaProducer)

    env.execute(s"Stream Producer for $topicName")
  }

  private def createSerializer(): GenericAvroSerializer[T] = {
    new GenericAvroSerializer[T](
      schemaString = schemaString,
      topic = topicName,
      toGenericRecord = toGenericRecord
    )
  }
}