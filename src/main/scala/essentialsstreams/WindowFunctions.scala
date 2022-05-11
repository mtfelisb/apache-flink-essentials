package essentialsstreams

import generators.Gaming._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, GlobalWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
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
  // [0, 3] [3, 6] [6, 9]

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

  /**
   * Keyed streams and Window functions
   */
  // each element will be assigned to another stream for its own key
  val streamByType: KeyedStream[ServerEvent, String] = source.keyBy(e => e.getClass.getSimpleName)

  // for every key there is a separate window allocation
  val threeSecondsTumblingWindowsByType = streamByType.window(TumblingEventTimeWindows.of(Time.seconds(3)))

  class CountByWindow extends WindowFunction[ServerEvent, String, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit =
      out.collect(s"$key: $window, ${input.size}")
  }

  def countByTypeByWindow(): Unit = {
    threeSecondsTumblingWindowsByType
      .apply(new CountByWindow)
      .print

    env.execute
  }

  // counting with ProcessWindowFunction
  class CountByWindowV2 extends ProcessWindowFunction[ServerEvent, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val window = context.window
      val registrationEventCount = elements.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"$key: $window, ${elements.size}")
    }
  }

  def countByTypeByWindowV2(): Unit = {
    threeSecondsTumblingWindowsByType
      .process(new CountByWindowV2)
      .print

    env.execute
  }

  // how many player were registered every 3 seconds, updated every 1s
  // [0, 3] [1, 4] [2, 5]

  def slidingAllWindows(): Unit = {
    val windowSize: Time = Time.seconds(3)
    val slidingTime: Time = Time.seconds(1)

    val slidingWindowsAll = source.windowAll(SlidingEventTimeWindows.of(windowSize, slidingTime))

    slidingWindowsAll
      .apply(new CountByWindowAll)
      .print

    env.execute
  }

  /**
   * Session windows
   *
   * Group of events with no more than
   * a certain time gap between them
   */

  // how many registration events is there with no more than 1s apart?

  def sessionWindows(): Unit = {
    val groupBySessionWindows = source.windowAll(EventTimeSessionWindows.withGap(Time.seconds(1)))

    // can use any kind of window function
    groupBySessionWindows.apply(new CountByWindowAll)
      .print

    env.execute
  }

  /**
   * Global window
   *
   */

  // how many registration events every 10 events

  // counting with AllWindowFunction
  class CountByGlobalWindowAll extends AllWindowFunction[ServerEvent, String, GlobalWindow] {
    override def apply(window: GlobalWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val registrationEventCount = input.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [${window}] $registrationEventCount")
    }
  }

  def globalWindow(): Unit = {
    val globalWindowEvents = source.windowAll(GlobalWindows.create())
      .trigger(CountTrigger.of[GlobalWindow](10))
      .apply(new CountByGlobalWindowAll)

    globalWindowEvents.print

    env.execute
  }

  /**
   * Exercise
   *
   * What was the time window when we had the most number
   * of registration events?
   */

  class KeepWindowAndCountFunction extends AllWindowFunction[ServerEvent, (TimeWindow, Long), TimeWindow] {
    override def apply(window: TimeWindow, input: Iterable[ServerEvent], out: Collector[(TimeWindow, Long)]): Unit =
      out.collect((window, input.size))
  }

  def exercise(): Unit = {
    val slidingWindows: DataStream[(TimeWindow, Long)] = source
      .filter(_.isInstanceOf[PlayerRegistered])
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(1)))
      .apply(new KeepWindowAndCountFunction)

    val localWindows: List[(TimeWindow, Long)] = slidingWindows.executeAndCollect().toList
    val bestWindows: (TimeWindow, Long) = localWindows.maxBy(_._2)

    println(s"The best window is ${bestWindows._1} with ${bestWindows._2} registration events")
  }

  def main(args: Array[String]): Unit = {
    exercise()
  }
}