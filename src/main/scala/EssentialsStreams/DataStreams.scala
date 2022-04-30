package EssentialsStreams

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

object DataStreams {

  // basic job template
  def basicJob(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // data source
    val simpleNumberStream: DataStream[Int] = env.fromElements(1, 2, 3, 4)

    // process
    simpleNumberStream.print

    // execute described computations
    env.execute
  }

  /**
   * Exercise: FizzBuzz on Flink
   *
   * Take a stream of 100 natural numbers for every number
   * - if n divisible of 3, then return "fizz"
   * - if n divisible of 5, then return "buzz"
   * - if n divisible of 3 and 5, then return "fizzbuzz"
   * - write the results in a file
   *
   */
  case class FizzBuzzResult(n: Long, output: String)

  def fizzBuzz(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // generate a source event
    val source: DataStream[Long] = env.fromSequence(1, 100)

    // process the data
    val results = source
      .map(n => n match { // cannot be anonymous
        case n if n % 3 == 0 && n % 5 == 0 => FizzBuzzResult(n, "fizzbuzz")
        case n if n % 3 == 0 => FizzBuzzResult(n, "fizz")
        case n if n % 5 == 0 => FizzBuzzResult(n, "buzz")
        case n => FizzBuzzResult(n, "")
      })
      .filter(_.output == "fizzbuzz")
      .map(_.n)

    // deprecated api
    // results.writeAsText("output/fizzbuzz.txt").setParallelism(1)

    // sink the results
    results.addSink(
      StreamingFileSink.forRowFormat(
        new Path("output/sink"),
        new SimpleStringEncoder[Long]("utf-8")
      ).build()
    ).setParallelism(1)

    // execute the job
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    fizzBuzz()
  }
}
