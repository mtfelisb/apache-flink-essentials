package essentialsstreams

import generators.Gaming.{PlayerRegistered, ServerEvent}
import generators.Shopping._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, Watermark, WatermarkGenerator, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Instant

object TimeBasedTransformations {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val shoppingCartEvents: DataStream[ShoppingCartEvent] = env.addSource(
    new ShoppingCartEventsGenerator(
      sleepMillisPerEvent = 100,
      batchSize = 5,
      baseInstant = Instant.parse("2022-02-15T00:00:00.000Z")
    )
  )

  // Event time = the moment the event was created
  // Processing time = the moment the event arives at flink

  class CountByWindowAll extends ProcessAllWindowFunction[ShoppingCartEvent, String, TimeWindow] {
    override def process(context: Context, elements: Iterable[ShoppingCartEvent], out: Collector[String]): Unit = {
      val window = context.window
      out.collect(s"Window [${window.getStart}] - [${window.getEnd}] ${elements.size}")
    }
  }

  // group by window every 3s
  // tumbling windows (non-overlapping)
  // processing time
  // doesnt matter when the event was created
  // multiple runs generate different results
  def processingTime(): Unit  = {
    val groupedEventsByWindow = shoppingCartEvents.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(3)))
    val countEventByWindow: DataStream[String] = groupedEventsByWindow.process(new CountByWindowAll)

    countEventByWindow.print
    env.execute
  }

  // handling late data - done with watermarks
  // doesnt care about flink internal time
  // results may be faster
  // same events + different runs = same results
  def eventTime(): Unit = {
    val groupedEventsByWindow = shoppingCartEvents
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500)) // max delay < 500 millis
          .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
            override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long =
              element.time.toEpochMilli
          })
      )
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))

    def countEventByWindow: DataStream[String] = groupedEventsByWindow.process(new CountByWindowAll)

    countEventByWindow.print
    env.execute
  }

  /**
   * Custom watermarks
   */

  // with every new max timestamp, every new incoming element with event time < max timestamp - max delay will be discarded
  class BoundedOutOfOrdernessGenerator(maxDelay: Long) extends WatermarkGenerator[ShoppingCartEvent] {
    var currentMaxTimestamp: Long = 0L

    // emitting a watermark its not mandatory
    override def onEvent(event: ShoppingCartEvent, eventTimestamp: Long, output: WatermarkOutput): Unit = {
      currentMaxTimestamp = Math.max(currentMaxTimestamp, event.time.toEpochMilli)

      // every new event older than this event will be discarded
      // output.emitWatermark(new Watermark(event.time.toEpochMilli))
    }

    // flink can also call onPeriodicEmit regularly
    // up to the engineer to emit a watermark at these times
    override def onPeriodicEmit(output: WatermarkOutput): Unit =
      output.emitWatermark(new Watermark(currentMaxTimestamp - maxDelay - 1))
  }

  def eventTimeV2(): Unit = {
    // control how often Flink calls onPeriodicEmit
    env.getConfig.setAutoWatermarkInterval(1000L) // call onPeriodicEmit every 1s

    val groupedEventsByWindow = shoppingCartEvents
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forGenerator(_ => new BoundedOutOfOrdernessGenerator(500L))
          .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
            override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long = element.time.toEpochMilli
          })
      )
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))

    val countEventsByWindow: DataStream[String] = groupedEventsByWindow.process(new CountByWindowAll)

    countEventsByWindow.print
    env.execute
  }

  def main(args: Array[String]): Unit = {
    eventTimeV2()
  }
}
