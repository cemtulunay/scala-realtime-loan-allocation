package generators

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.watermark.Watermark
import java.util.UUID
import java.time.Instant
import scala.annotation.tailrec


case class incomePredictionRequest(
                                    requestId: Option[String],
                                    applicationId: Option[String],
                                    customerId: Option[String],
                                    prospectId: Option[String],
                                    requestedAt: Instant,
                                    incomeSource: String  // "real-time" for prospects, "batch" for customers
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
      ctx.emitWatermark(new Watermark(event.requestedAt.toEpochMilli))
      Thread.sleep(sleepMillisPerEvent)
      if (id % 10 == 0) extraDelayInMillisOnEveryTenEvents.foreach(Thread.sleep)
      run(id + 1, ctx)
    }
  }

  private def generateEvent(id: Long): incomePredictionRequest = {
    val isProspect = scala.util.Random.nextBoolean()

    incomePredictionRequest(
      requestId = Some(UUID.randomUUID().toString),
      applicationId = Some(UUID.randomUUID().toString),
      customerId = if (!isProspect) Some(UUID.randomUUID().toString) else None,
      prospectId = if (isProspect) Some(UUID.randomUUID().toString) else None,
      requestedAt = baseInstant.plusSeconds(id),
      incomeSource = if (isProspect) "real-time" else "batch"
    )
  }

  override def run(ctx: SourceFunction.SourceContext[incomePredictionRequest]): Unit = run(1, ctx)

  override def cancel(): Unit = {
    running = false
  }
}