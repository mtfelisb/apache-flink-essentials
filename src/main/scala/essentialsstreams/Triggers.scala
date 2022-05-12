package essentialsstreams

import generators.Shopping.{ShoppingCartEvent, ShoppingCartEventsGenerator}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Triggers {

  // when a window function is executed

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  // runs the process function twice
  def countTrigger(): Unit = {
    env
      .addSource(new ShoppingCartEventsGenerator(500, 2)) // 2 events per second
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 10 events/window
      .trigger(CountTrigger.of[TimeWindow](5)) // the window function runs every 5 elements
      .process(new CountByWindowAll)
      .print

    env execute
  }

  // purge the window when it fires
  def purgingTrigger(): Unit = {
    env
      .addSource(new ShoppingCartEventsGenerator(500, 2)) // 2 events per second
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 10 events/window
      .trigger(PurgingTrigger.of(CountTrigger.of[TimeWindow](5))) // the window function runs every 5 elements
      .process(new CountByWindowAll)
      .print

    env execute
  }

  // other triggers
  // EventTimeTrigger       - happens by default when the watermark is > window end time (automatic for time windows)
  // ProcessingTimeTrigger  - fires when the current system time > window end time (automatic for processing time windows)
  // custom triggers        - powerful APIs for custom firing behavior

  def main(args: Array[String]): Unit = {
    purgingTrigger
  }
}

// copied from Time Based Transformations
class CountByWindowAll extends ProcessAllWindowFunction[ShoppingCartEvent, String, TimeWindow] {
  override def process(context: Context, elements: Iterable[ShoppingCartEvent], out: Collector[String]): Unit = {
    val window = context.window
    out.collect(s"Window [${window.getStart} - ${window.getEnd}] ${elements.size}")
  }
}