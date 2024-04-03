package state

import generators.shopping._
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RichFunctions {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val numberStream: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5, 6)

  // Pure FP
  val tenXNumbers = numberStream.map(_ * 10)

  // "Explicit" Map Functions
  val tenXNumbers2: DataStream[Int] = numberStream.map(new MapFunction[Int, Int] {
    override def map(value: Int): Int = value * 10
  })

  // Rich map function - implement abstract rich function, allowing access to rich details, runtime, cluster, open, close
  val tenXNumbers3: DataStream[Int] = numberStream.map(new RichMapFunction[Int, Int] {
    override def map(value: Int): Int = value * 10
  })
  private val tenXNumbers3WithLifeCycle: DataStream[Int] = numberStream.map(new RichMapFunction[Int, Int] {
    override def map(value: Int): Int = value * 10 // mandatory override

    // optional override
    // called before the data goes though
    override def open(parameters: Configuration): Unit = {
      println("Starting my work!")
    }

    // called after all the data
    override def close(): Unit = {
      println("Finishing my work!")
    }

  })
  /*Rich function is instantiated even before the data is being processed*/
  private val tenXNumbers3WithLifeCycle2: DataStream[Int] = numberStream.map(new RichMapFunction[Int, Int] {
    override def map(value: Int): Int = value * 10 // mandatory override

    // optional override
    // called before the data goes though
    override def open(parameters: Configuration): Unit = {
      println("Starting second my work!")
    }

    // called after all the data
    override def close(): Unit = {
      println("Finishing second my work!")
    }

  })

  /*
  ProcessFunction is the most general function abstraction in Flink
  */

  val tenXNumbersProcess = numberStream.process(new ProcessFunction[Int, Int] {
    override def processElement(value: Int, ctx: ProcessFunction[Int, Int]#Context, out: Collector[Int]): Unit = {
      out.collect(value * 10) //manually push the value out of stream
    }

    //also can override lifecycle
    override def open(parameters: Configuration): Unit = {
      println("Starting process function")
    }

    override def close(): Unit = {
      println("Closing process function")
    }
  })


  // WE HAVE FOUR LAYERS OF ABSTRACTIONS FOR A SIMPLE MAP TRANFORMATION
  // It can be applied to flatMap as well... obviously

  /*Exercise
  * explode all purchase events to a single item
  * [("boots", 2), ("iPhone", 1)] ->
  * ["boots", "boots", "iPhone"]
  * - lambdas
  * - explicit functions
  * - rich functions
  * - process functions
  * */

  private def exercise(): Unit = {
    val exerciseEnv = StreamExecutionEnvironment.getExecutionEnvironment
    exerciseEnv.setParallelism(1)
    val shoppingCartStream: DataStream[AddToShoppingCartEvent] =
      exerciseEnv.addSource(new SingleShoppingCartEventsGenerator(100)) //10 events/sec
        .filter(_.isInstanceOf[AddToShoppingCartEvent])
        .map(_.asInstanceOf[AddToShoppingCartEvent])

    // 1. lambdas
    val itemsPurchasedStream = shoppingCartStream.flatMap(event => (1 to event.quantity).map(_ => event.sku))
    // 2. explicit flatmap function
    val itemsPurchasedStream2 = shoppingCartStream.flatMap(new FlatMapFunction[AddToShoppingCartEvent, String] {
      override def flatMap(event: AddToShoppingCartEvent, out: Collector[String]): Unit = {
        (1 to event.quantity).map(_ => event.sku).foreach(out.collect)
      }
    })
    // 3. rich flatmap function
    val itemsPurchasedStream3 = shoppingCartStream.flatMap(new RichFlatMapFunction[AddToShoppingCartEvent, String] {
      override def flatMap(event: AddToShoppingCartEvent, out: Collector[String]): Unit = {
        (1 to event.quantity).map(_ => event.sku).foreach(out.collect)
      }

      override def open(parameters: Configuration): Unit = println("Opening flat map rich function")

      override def close(): Unit = println("Closing flat map rich function")
    })
    // 4. processFn
    val itemsPurchasedStream4 = shoppingCartStream.process(new ProcessFunction[AddToShoppingCartEvent, String] {
      override def processElement(event: AddToShoppingCartEvent,
                                  ctx: ProcessFunction[AddToShoppingCartEvent, String]#Context,
                                  out: Collector[String]): Unit = {
        (1 to event.quantity).map(_ => event.sku).foreach(out.collect)
      }
    })


    itemsPurchasedStream3.print()
    exerciseEnv.execute()
  }

  def main(args: Array[String]): Unit = {
    // tenXNumbers3WithLifeCycle.print()
    exercise()
    env.execute()
  }

}
