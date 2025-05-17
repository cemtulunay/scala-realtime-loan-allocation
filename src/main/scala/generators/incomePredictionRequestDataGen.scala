package generators

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.watermark.Watermark

import java.util.UUID
import java.time.Instant
import scala.annotation.tailrec
import scala.util.Random


case class predictionRequest(
                                    requestId: Option[String],
                                    applicationId: Option[String],
                                    customerId: Option[Int],
                                    prospectId: Option[Int],
                                    incomeRequestedAt: Option[Long],
                                    e1ProducedAt: Option[Long],
                                    incomeSource: String,                        // "real-time" for prospects, "batch" for customers
                                    isCustomer: Boolean,
                                    predictedIncome: Option[Double]
                                  )

class predictionRequestGenerator(
                                        sleepMillisPerEvent: Int,
                                        baseInstant: Instant = Instant.now(),
                                        extraDelayInMillisOnEveryTenEvents: Option[Long] = None,
                                        isCustomerParameter: Boolean = true
                                      ) extends RichParallelSourceFunction[predictionRequest] {

  @volatile private var running = true

  @tailrec
  private def run(id: Long, ctx: SourceFunction.SourceContext[predictionRequest]): Unit = {
    if (running) {
      val event = generateEvent(id)
      ctx.collect(event)
      ctx.emitWatermark(new Watermark(event.incomeRequestedAt.getOrElse(0L)))
      Thread.sleep(sleepMillisPerEvent)
      if (id % 10 == 0) extraDelayInMillisOnEveryTenEvents.foreach(Thread.sleep)
      run(id + 1, ctx)
    }
  }

  private def generateEvent(id: Long): predictionRequest = {
    // val isCustomer = Random.nextDouble() < 0.8 // 80% chance of being a customer
    val isCustomer:Boolean = isCustomerParameter
    val customerId = if (isCustomer) Some(Random.nextInt(999999) + 1000000) else Some(0)
    val oneDayAgo = System.currentTimeMillis() - (24 * 60 * 60 * 1000) // 24 hours ago in milliseconds - This will immitate the batch operation time

    predictionRequest(
      requestId = Some(UUID.randomUUID().toString),
      applicationId = Some(UUID.randomUUID().toString),
      customerId = customerId,
      prospectId = if (!isCustomer) Some(Random.nextInt(999999) + 2000000) else Some(0),
      incomeRequestedAt = if (!isCustomer) Some(baseInstant.plusSeconds(id).toEpochMilli) else Some(oneDayAgo),
      e1ProducedAt = Some(baseInstant.plusSeconds(id).toEpochMilli),
      incomeSource = if (!isCustomer) "real-time" else "batch",
      isCustomer = isCustomer,
      predictedIncome = if (!isCustomer) Some(0) else Some(50000.0 + (customerId.getOrElse(0) % 10) * 5000.0)
    )
  }

  override def run(ctx: SourceFunction.SourceContext[predictionRequest]): Unit = run(1, ctx)

  override def cancel(): Unit = {
    running = false
  }

}