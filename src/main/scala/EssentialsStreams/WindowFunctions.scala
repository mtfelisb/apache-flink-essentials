package EssentialsStreams

import Generators.Gaming._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

import java.time.Instant
import scala.concurrent.duration.DurationInt

object WindowFunctions {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  // datasource server started at
  implicit val serverStartTime: Instant = Instant.parse("2022-02-02T00:00:00.000Z")

  // emulate a time series list of events
  val events: List[ServerEvent] = List(
    bob.register(2.seconds),
    bob.online(2.seconds),
    sam.register(3.seconds),
    sam.online(4.seconds),
    rob.register(4.seconds),
    alice.register(4.seconds),
    mary.register(6.seconds),
    mary.online(6.seconds),
    carl.register(8.seconds),
    rob.online(10.seconds),
    alice.online(10.seconds),
    carl.online(10.seconds)
  )

  // mocked data source
  val source: DataStream[ServerEvent] = env
    .fromCollection(events)
    // extracting the event timestamps and watermarks
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))
        .withTimestampAssigner(new SerializableTimestampAssigner[ServerEvent] {
          override def extractTimestamp(element: ServerEvent, recordTimestamp: Long): Long = element.eventTime.toEpochMilli
        })
    )

  // how many players were registered every 3s?
  val threeSecondsTumblingWindow: AllWindowedStream[ServerEvent, TimeWindow] =
      source.windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))

  // counting with AllWindowFunction
  class CountByWindowAll extends AllWindowFunction[ServerEvent, String, TimeWindow] {
    override def apply(window: TimeWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val registrationEventCount = input.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [${window.getStart} - [${window.getEnd}] $registrationEventCount")
    }
  }

  def countByWindow(): Unit = {
    val registrationPerThreeSeconds = threeSecondsTumblingWindow.apply(new CountByWindowAll)
    registrationPerThreeSeconds.print()
    env.execute()
  }

  // counting with ProcessAllWindowFunction
  class CountByWindowAllV2 extends ProcessAllWindowFunction[ServerEvent, String, TimeWindow] {
    override def process(context: Context, elements: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val window = context.window
      val registrationEventCount = elements.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [${window.getStart} - [${window.getEnd}] $registrationEventCount")
    }
  }

  def countByWindowV2(): Unit = {
    val registrationPerThreeSeconds = threeSecondsTumblingWindow.process(new CountByWindowAllV2)
    registrationPerThreeSeconds.print()
    env.execute()
  }

  // counting with AggregateFunction
  class CountByWindowV3 extends AggregateFunction[ServerEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: ServerEvent, accumulator: Long): Long =
      if (value.isInstanceOf[PlayerRegistered]) accumulator + 1
      else accumulator

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  def countByWindowV3(): Unit = {
    val registrationPerThreeSeconds = threeSecondsTumblingWindow.aggregate(new CountByWindowV3)
    registrationPerThreeSeconds.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    countByWindowV3()
  }
}