package generators

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.watermark.Watermark

import java.util.UUID
import java.time.Instant
import scala.annotation.tailrec
import scala.util.Random


case class incomePredictionRequest(
                                    requestId: Option[String],
                                    applicationId: Option[String],
                                    customerId: Option[Int],
                                    prospectId: Option[Int],
                                    requestedAt: Option[Long],
                                    incomeSource: String,          // "real-time" for prospects, "batch" for customers
                                    isCustomer: Boolean
                                  )

class incomePredictionRequestGenerator(
                                        sleepMillisPerEvent: Int,
                                        baseInstant: Instant = Instant.now(),
                                        extraDelayInMillisOnEveryTenEvents: Option[Long] = None
                                      ) extends RichParallelSourceFunction[incomePredictionRequest] {

  @volatile private var running = true

  @tailrec
  private def run(id: Long, ctx: SourceFunction.SourceContext[incomePredictionRequest]): Unit = {
    if (running) {
      val event = generateEvent(id)
      ctx.collect(event)
      ctx.emitWatermark(new Watermark(event.requestedAt.getOrElse(0L)))
      Thread.sleep(sleepMillisPerEvent)
      if (id % 10 == 0) extraDelayInMillisOnEveryTenEvents.foreach(Thread.sleep)
      run(id + 1, ctx)
    }
  }

  private def generateEvent(id: Long): incomePredictionRequest = {
    val isCustomer = Random.nextDouble() < 0.8 // 80% chance of being a customer

    incomePredictionRequest(
      requestId = Some(UUID.randomUUID().toString),
      applicationId = Some(UUID.randomUUID().toString),
      customerId = if (isCustomer) Some(Random.nextInt(999999) + 1000000) else Some(0),
      prospectId = if (!isCustomer) Some(Random.nextInt(999999) + 2000000) else Some(0),
      requestedAt = Some(baseInstant.plusSeconds(id).toEpochMilli),
      incomeSource = if (isCustomer) "real-time" else "batch",
      isCustomer
    )
  }

  override def run(ctx: SourceFunction.SourceContext[incomePredictionRequest]): Unit = run(1, ctx)

  override def cancel(): Unit = {
    running = false
  }
}