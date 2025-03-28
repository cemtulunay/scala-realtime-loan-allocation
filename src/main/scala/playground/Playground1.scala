package playground

import org.apache.flink.streaming.api.scala._
import org.apache.flink._

object Playground1 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements(1 to 1000: _*)
    data.print()
    env.execute()
  }
}