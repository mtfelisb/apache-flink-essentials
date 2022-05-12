package essentialsstreams

import generators.Shopping.{CatalogEvent, CatalogEventsGenerator, ShoppingCartEvent, SingleShoppingCartEventsGenerator}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, ProcessJoinFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object MultipleStreams {

  // union
  // window join
  // interval join
  // connect

  // union = combining the output of multiple streams into one
  def union(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shoppingCartEventsKafka: DataStream[ShoppingCartEvent] =
      env.addSource(new SingleShoppingCartEventsGenerator(300, sourceId = Option("kafka")))

    val shoppingCartEventsFiles: DataStream[ShoppingCartEvent] =
      env.addSource(new SingleShoppingCartEventsGenerator(300, sourceId = Option("files")))

    shoppingCartEventsKafka
      .union(shoppingCartEventsFiles)
      .print

    env execute
  }

  // window join = elements belong to the same window + some join condition
  def windowJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shoppingCartEvent = env.addSource(new SingleShoppingCartEventsGenerator(1000, sourceId = Option("kafka")))
    val catalogEvents = env.addSource(new CatalogEventsGenerator(200))

    shoppingCartEvent
      .join(catalogEvents)
      // providing a join condition
      .where(_.userId)
      .equalTo(_.userId)
      // providing the same window grouping
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      // process the correlated events
      .apply(
        (shoppingCartEvent, catalogEvent) =>
          s"User ${shoppingCartEvent.userId} browsed at ${catalogEvent.time} and bought at ${shoppingCartEvent.time}"
      )
      .print

    env execute
  }

  // interval joins = correlation between events A and B if durationMin < timeA - timeB < durationMax
  // involves event time and only works on keyed streams
  def intervalJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shoppingCartEvents =
      env.addSource(new SingleShoppingCartEventsGenerator(300, sourceId = Option("kafka")))
        .assignTimestampsAndWatermarks(
          WatermarkStrategy.forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))
            .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
              override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long) =
                element.time.toEpochMilli
            })
        )
        .keyBy(_.userId)

    val catalogEvents = env.addSource(new CatalogEventsGenerator(500))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))
          .withTimestampAssigner(new SerializableTimestampAssigner[CatalogEvent] {
            override def extractTimestamp(element: CatalogEvent, recordTimestamp: Long) =
              element.time.toEpochMilli
          })
      )
      .keyBy(_.userId)

    shoppingCartEvents
      .intervalJoin(catalogEvents)
      .between(Time.seconds(-2), Time.seconds(2))
      // interval is by default inclusive
      .lowerBoundExclusive()
      .upperBoundExclusive()
      .process(new ProcessJoinFunction[ShoppingCartEvent, CatalogEvent, String] {
        override def processElement(
          left: ShoppingCartEvent,
          right: CatalogEvent,
          ctx: ProcessJoinFunction[ShoppingCartEvent, CatalogEvent, String]#Context,
          out: Collector[String]
        ) =
          out.collect(s"User ${left.userId} browsed at ${right.time} and bought at ${left.time}")
      })
      .print

    env execute
  }

  // connect = two streams are treated with the same operator
  def connect(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shoppingCartEvents = env.addSource(new SingleShoppingCartEventsGenerator(100)).setParallelism(1)
    val catalogEvents = env.addSource(new CatalogEventsGenerator(1000)).setParallelism(1)

    val connectedStream: ConnectedStreams[ShoppingCartEvent, CatalogEvent] = shoppingCartEvents.connect(catalogEvents)

    // variables - will use single-threaded
    env.setParallelism(1)
    env.setMaxParallelism(1)

    connectedStream.process(
      new CoProcessFunction[ShoppingCartEvent, CatalogEvent, Double] {
        var shoppingCartEventCount = 0
        var catalogEventCount = 0

        override def processElement1(
          value: ShoppingCartEvent,
          ctx: CoProcessFunction[ShoppingCartEvent, CatalogEvent, Double]#Context,
          out: Collector[Double]
        ) = {
          shoppingCartEventCount += 1
          out.collect(shoppingCartEventCount * 100.0 / (shoppingCartEventCount + catalogEventCount))
        }

        override def processElement2(
          value: CatalogEvent,
          ctx: CoProcessFunction[ShoppingCartEvent, CatalogEvent, Double]#Context,
          out: Collector[Double]
        ) = {
          catalogEventCount += 1
          out.collect(shoppingCartEventCount * 100.0 / (shoppingCartEventCount + catalogEventCount))
        }
      }
    )
    .print

    env execute
  }

  def main(args: Array[String]): Unit = {
    connect()
  }
}
