package source

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.functions.source.SourceFunction

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

/**
 * A generic PostgreSQL source that can read any type of record from a PostgreSQL database.
 *
 * @param jdbcUrl The JDBC URL to connect to
 * @param username Database username
 * @param password Database password
 * @param query The SQL query to execute
 * @param recordMapper Function to map a ResultSet to a record of type T
 * @param updateQuery Optional query to update the processed record (e.g., mark as processed)
 * @param updateQueryParamSetter Optional function to set parameters for the update query
 * @param pollingInterval Interval in milliseconds between polling cycles
 * @tparam T The type of records to produce
 */
class GenericPostgreSQLSource[T](
                                  jdbcUrl: String,
                                  username: String,
                                  password: String,
                                  query: String,
                                  recordMapper: ResultSet => T,
                                  updateQuery: Option[String] = None,
                                  updateQueryParamSetter: Option[(PreparedStatement, T) => Unit] = None,
                                  pollingInterval: Long = 100,
                                  private val typeInfo: TypeInformation[T]
                                ) extends SourceFunction[T] with ResultTypeQueryable[T] with Serializable {

  @volatile private var connection: Connection = _
  @volatile private var preparedStatement: PreparedStatement = _
  @volatile private var isRunning: Boolean = true

  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    try {
      // Initialize connection
      connection = DriverManager.getConnection(
        jdbcUrl,
        username,
        password
      )

      preparedStatement = connection.prepareStatement(query)

      while (isRunning) {
        val resultSet = preparedStatement.executeQuery()

        while (resultSet.next()) {
          val record = recordMapper(resultSet)
          ctx.collect(record)

          // Update the record if an update query is provided
          (updateQuery, updateQueryParamSetter) match {
            case (Some(uQuery), Some(paramSetter)) => updateRecord(uQuery, record, paramSetter)
            case _ => // No update query or param setter
          }
        }

        resultSet.close()
        Thread.sleep(pollingInterval)
      }
    } catch {
      case e: Exception =>
        println(s"Error in GenericPostgreSQLSource: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      cleanup()
    }
  }

  private def updateRecord(updateQuery: String, record: T, paramSetter: (PreparedStatement, T) => Unit): Unit = {
    var updateStmt: PreparedStatement = null
    try {
      updateStmt = connection.prepareStatement(updateQuery)
      paramSetter(updateStmt, record)
      updateStmt.executeUpdate()
    } finally {
      if (updateStmt != null) {
        updateStmt.close()
      }
    }
  }

  private def cleanup(): Unit = {
    if (preparedStatement != null) {
      preparedStatement.close()
    }
    if (connection != null) {
      connection.close()
    }
  }

  override def cancel(): Unit = {
    isRunning = false
    cleanup()
  }

  override def getProducedType: TypeInformation[T] = typeInfo
}