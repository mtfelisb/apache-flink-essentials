package EssentialsStreams

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, ReduceFunction}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object Transformations {
  def explicitTransformations(): Unit = {

    // acquires execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // data source
    val source: DataStream[Long] = env.fromSequence(1, 4)

    // straightforward map
    val doubledSource = source.map(_ * 2)

    // explicit map approach
    val doubleSourceV2 = source.map(new MapFunction[Long, Long] {
      // allow to declare fields, methods and so on
      override def map(value: Long): Long = value * 2
    })

    // straightforward flatmap
    val expandedSource = source.flatMap(n => Range.Long(1, n, 1).toList)

    // explicit flatMap approach
    val expandedSourceV2 = source.flatMap(new FlatMapFunction[Long, Long] {
      // allow to declare fields, methods and so on
      override def flatMap(value: Long, out: Collector[Long]): Unit = {
        // imperative style
        // it pushes new elements downstream
        Range.Long(1, value, 1).foreach { n => out.collect(n) }
      }
    })

    // process method
    // the most general function to process elements in Flink
    val expandedSourceV3 = source.process(new ProcessFunction[Long, Long] {
      override def processElement(value: Long, ctx: ProcessFunction[Long, Long]#Context, out: Collector[Long]): Unit = {
        Range.Long(1, value, 1).foreach { n => out.collect(n) }
      }
    })

    // reduce
    val keyedNumbers: KeyedStream[Long, Boolean] = source.keyBy(n => n % 2 == 0)

    // sum all elements by key
    val sumByKey = keyedNumbers.reduce(_ + _)

    // explicit reduce approach
    val sumByKeyV2 = keyedNumbers.reduce(new ReduceFunction[Long] {
      override def reduce(x: Long, y: Long): Long = x + y
    })

    sumByKey.print

    env.execute
  }

  def main(args: Array[String]): Unit = {
    explicitTransformations()
  }
}
