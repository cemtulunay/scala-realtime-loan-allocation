package loan

import generators.{incomePredictionRequest, incomePredictionRequestGenerator}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object loanDecisionService_20240329 {

  def demoValueState(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val incomePredictionRequestEvents = env.addSource(
      new incomePredictionRequestGenerator(
        sleepMillisPerEvent = 100, // ~ 10 events/s
      )
    )

    //val eventsPerRequest: KeyedStream[incomePredictionRequest, String] = incomePredictionRequestEvents.keyBy(_.requestId.getOrElse("unknown"))
    val eventsPerRequest: KeyedStream[incomePredictionRequest, String] = incomePredictionRequestEvents.keyBy(_.customerId.getOrElse("0"))

    /** ****************************************************************************************************************************************************
     *  2-) STATEFUL APPROACH - State Primitives - ValueState - Distributed Available
     * **************************************************************************************************************************************************** */

    val numEventsPerRequestStream = eventsPerRequest.process(
      new KeyedProcessFunction[String, incomePredictionRequest, String] {

        var stateCounter: ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          // initialize all state
          val descriptor = new ValueStateDescriptor[Long]("events-counter", classOf[Long])
          stateCounter = getRuntimeContext.getState(descriptor)
        }

        override def processElement(
                                     value: incomePredictionRequest,
                                     ctx: KeyedProcessFunction[String, incomePredictionRequest, String]#Context,
                                     out: Collector[String]
                                   ): Unit = {

          val currentState = Option(stateCounter.value()).getOrElse(0L) // If state is null, use 0L

          // Update state with the new count
          stateCounter.update(currentState + 1)

          // Collect the output
          out.collect(s"request ${value.requestId.getOrElse("unknown")} - ${currentState + 1} - customer ${value.customerId.getOrElse("0")}")
        }
      }
    )

    numEventsPerRequestStream.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    demoValueState()
  }
}


