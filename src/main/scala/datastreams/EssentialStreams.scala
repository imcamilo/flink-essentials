package datastreams

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, ReduceFunction}
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object EssentialStreams {

  private def applicationTemplate(): Unit = {
    // 1. execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // in between add any sort of computations

    //import org.apache.flink.streaming.api.scala._
    // import TypeInformation for the data fo your DataStreams

    val simpleMemberStream: DataStream[Int] = env.fromElements(1, 2, 3, 4)

    // perform some actions
    simpleMemberStream.print()

    // at the end
    env.execute() // trigger all the computations that were described earlier
    //printed out of order bcs its parallel
  }

  // transformations
  private def demoTransformations(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._ // import TypeInformation
    val n: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5)

    //checking parallelism
    println(s"current parallelism ${env.getParallelism}")
    //set parallelism
    env.setParallelism(2)
    println(s"current parallelism ${env.getParallelism}")
    //you can set a different parallelism to every step
    val doubleNumbers: DataStream[Int] = n.map(_ * 2)
    val expandedNumbers: DataStream[Int] = n.flatMap(n => List(n, n + 1))
    val filteredNumbers: DataStream[Int] = n.filter(_ % 2 == 0) /*you can set parallelism here*/
      .setParallelism(4)
    val finalData = expandedNumbers.writeAsText("output/expandedStream.txt")
    finalData.setParallelism(3) //set parallelism to this particular task
    env.execute()
  }

  /*
  * Exercise, Fizzbuzz on Flink
  * - take a stream of 100natural numbers
  * - for every number
  *   - if n % 3 == 0 then return "fizz"
  *   - if n % 5 == 0 then return "buzz"
  *   - if both then return "fizzbuzz"
  * - write the numbers for which you said "fizzbuzz" to a file
  *
  * */

  private def fizzBuzz(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val naturals: DataStream[Int] = env.fromCollection(1 to 100)

    val fizzBuzz = naturals.map((n: Int) => n match {
      case n if n % 3 == 0 && n % 5 == 0 => (n, "fizzbuzz")
      case n if n % 3 == 0 => (n, "fizz")
      case n if n % 5 == 0 => (n, "buzz")
      case n => (n, "")
    }
    )

    //deprecated way to write a file
    fizzBuzz.filter(f => f._2 == "fizzbuzz").writeAsText("output/fizzbuzz.txt").setParallelism(1)

    // ADD a sink
    //non deprecated way to write a file
    fizzBuzz.map(_._1).addSink(
      StreamingFileSink.forRowFormat(
          new Path("output/streaming_sink"),
          new SimpleStringEncoder[Int]("UTF-8")
        )
        .build()
    ).setParallelism(1)

    env.execute()
  }

  // explicit transformations
  def demoExplicitTransformations(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val numbers = env.fromSequence(1, 100)

    // MAP
    //scala standard library
    val double1 = numbers.map(_ * 2)
    //flink has a lower level construct for map function
    //explicit version
    val double2 = numbers.map(new MapFunction[Long, Long] {
      // the advantage would be that you can use, declare fields, members, method, etc.
      override def map(value: Long): Long = value * 2
    })

    // FLAT MAP
    //scala standard lib
    val flatMapper1 = numbers.flatMap(n => Range.Long(1, n, 1).toList)
    //explicit
    val flatMappers2 = numbers.flatMap(new FlatMapFunction[Long, Long] {
      // the advantage would be that you can use, declare fields, members, method, etc.
      //collector is stateful, Collector is a data structure with imperative methods
      override def flatMap(value: Long, out: Collector[Long]): Unit =
        //why do this? just why?
        Range.Long(1, value, 1).toList.foreach { i =>
          out.collect(i) //imperative style - pushes the new element downstream for flink to process later.
        }
    })

    // PROCESS
    // PROCESS FUNCTION is the most general function to process elements in Flink
    val expandedNumbers1 = numbers.process(new ProcessFunction[Long, Long] {
      // ctx, ProcessFunction its a very rich data structure, with a bunch of methods and APIs to access the internal
      // flink state
      // and collector pushes elements downstream
      override def processElement(value: Long, ctx: ProcessFunction[Long, Long]#Context, out: Collector[Long]): Unit = {
        Range.Long(1, value, 1).toList.foreach { i =>
          out.collect(i) //imperative style - pushes the new element downstream for flink to process later.
        }
      }
    })

    //REDUCE
    // Happens in Keyed Streams, are essentially hash maps that are streaming
    val keyedNumbers: KeyedStream[Long, Boolean] = numbers.keyBy(n => n % 2 == 0)
    // reduce by FP approach
    val sumByKey = keyedNumbers.reduce(_ + _)
    // reduce by explicit approach
    val sumByKey2 = keyedNumbers.reduce(new ReduceFunction[Long] {
      // additional fields, methods, etc
      override def reduce(x: Long, y: Long): Long = x + y
    })
    sumByKey2.print()
    env.execute()

  }

  def main(args: Array[String]): Unit = {
    applicationTemplate()
    // demoTransformations()
    // fizzBuzz()
    //demoExplicitTransformations()
  }

}
