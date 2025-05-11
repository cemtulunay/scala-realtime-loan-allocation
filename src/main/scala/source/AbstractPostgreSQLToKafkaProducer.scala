package source

import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import serializer.GenericAvroSerializer

import java.sql.{PreparedStatement, ResultSet}
import java.util.Properties

/**
 * Abstract base class for PostgreSQL to Kafka producers using Flink.
 * This class provides a template for creating specific producers by extending it.
 *
 * @tparam T The type of records to produce
 */
abstract class AbstractPostgreSQLToKafkaProducer[T] extends Serializable {

  // Abstract methods to be implemented by subclasses
  protected def getJobName: String
  protected def getJdbcUrl: String
  protected def getJdbcUsername: String
  protected def getJdbcPassword: String
  protected def getSelectQuery: String
  protected def getUpdateQuery: Option[String] = None
  protected def getUpdateQueryParamSetter: Option[(PreparedStatement, T) => Unit] = None
  protected def getRecordMapper: ResultSet => T
  protected def getPollingInterval: Long = 100
  protected def getKafkaTopic: String
  protected def getAvroSchema: String
  protected def getToGenericRecord: (T, GenericRecord) => Unit
  protected def getKeyExtractor: Option[T => Array[Byte]] = None
  protected def getKafkaProperties: Properties
  protected def getCheckpointInterval: Long = 10000
  protected def shouldPrintResults: Boolean = true
  protected def getTypeInformation: TypeInformation[T]

  /**
   * Main execution method that sets up and runs the Flink job.
   */
  def execute(): Unit = {
    // Setup Environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(getCheckpointInterval)

    // Create the source
    val source = new GenericPostgreSQLSource[T](
      jdbcUrl = getJdbcUrl,
      username = getJdbcUsername,
      password = getJdbcPassword,
      query = getSelectQuery,
      recordMapper = getRecordMapper,
      updateQuery = getUpdateQuery,
      updateQueryParamSetter = getUpdateQueryParamSetter,
      pollingInterval = getPollingInterval,
      typeInfo = getTypeInformation
    )

    val resultStream = env.addSource(source)

    // Create the serializer
    val serializer = new GenericAvroSerializer[T](
      schemaString = getAvroSchema,
      topic = getKafkaTopic,
      toGenericRecord = getToGenericRecord,
      keyExtractor = getKeyExtractor
    )

    // Add Kafka sink
    resultStream.addSink(new FlinkKafkaProducer[T](
      getKafkaTopic,
      serializer,
      getKafkaProperties,
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE
    ))

    // Optionally print the stream for debugging
    if (shouldPrintResults) {
      resultStream.print()
    }

    // Execute the job
    env.execute(getJobName)
  }
}