package datastreams

import generators.shopping._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, ProcessJoinFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/*
* - Union
* - Window Joins
* - Interval Joins
* - Connect
* */
object MultipleStreams {


  // UNIONING
  // combining the output of multiple streams into just one
  private def demoUnion(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //define 2 streams of the same type
    val shoppingCartEventsKafka: DataStream[ShoppingCartEvent] =
      env.addSource(new SingleShoppingCartEventsGenerator(300, sourceId = Option("kafka")))
    val shoppingCartEventsFiles: DataStream[ShoppingCartEvent] =
      env.addSource(new SingleShoppingCartEventsGenerator(1000, sourceId = Option("files")))
    val combinedShoppingCartEventsStream: DataStream[ShoppingCartEvent] =
      shoppingCartEventsKafka.union(shoppingCartEventsFiles)
    combinedShoppingCartEventsStream.print()
    env.execute()
  }

  // WINDOW JOIN
  // its only possible if you provide the same window grouping and join condition
  // elements belong to the same window + some join condition
  private def demoWindowJoins(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val shoppingCartEvents = env.addSource(new SingleShoppingCartEventsGenerator(1000, sourceId = Option("kafka")))
    val catalogEvents = env.addSource(new CatalogEventsGenerator(1000))
    // join and provided a join condition and a window condition
    val joinedStream = shoppingCartEvents
      .join(catalogEvents)
      .where(shoppingCartEvent => shoppingCartEvent.userId)
      .equalTo(catalogEvent => catalogEvent.userId)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      //do something with correlated events
      .apply(
        (shoppingCartEvent, catalogEvent) => s"User ${shoppingCartEvent.userId} " +
          s"browsed at ${catalogEvent.time} and bought at ${catalogEvent.time}")

    joinedStream.print()
    env.execute()

  }

  // INTERVAL JOIN
  // correlation between events A and B if durationMin < timeA - timeB > durationMax
  // involves event time!
  // only works on Keyed Streams
  def demoIntervalJoin(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val shoppingCartEvents = env
      .addSource(new SingleShoppingCartEventsGenerator(300, sourceId = Option("kafka")))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness(java.time.Duration.ofMillis(5000))
          .withTimestampAssigner(
            new SerializableTimestampAssigner[ShoppingCartEvent] {
              override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long = {
                element.time.toEpochMilli
              }
            }
          )
      ).keyBy(_.userId)

    val catalogEvents = env.addSource(new CatalogEventsGenerator(500))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness(java.time.Duration.ofMillis(5000))
          .withTimestampAssigner(
            new SerializableTimestampAssigner[CatalogEvent] {
              override def extractTimestamp(element: CatalogEvent, recordTimestamp: Long): Long = {
                element.time.toEpochMilli
              }
            }
          )
      ).keyBy(_.userId)

    val intervalJoinStream = shoppingCartEvents
      .intervalJoin(catalogEvents)
      .between(Time.seconds(-2), Time.seconds(2))
      .lowerBoundExclusive() //interval its by default inclusive
      .upperBoundExclusive()
      // 3 types arguments, First EventType, Second EventType and Output Type
      .process(new ProcessJoinFunction[ShoppingCartEvent, CatalogEvent, String] {
        override def processElement(left: ShoppingCartEvent,
                                    right: CatalogEvent,
                                    ctx: ProcessJoinFunction[ShoppingCartEvent,
                                      CatalogEvent, String]#Context,
                                    out: Collector[String]): Unit = {
          out.collect(s"User ${left.userId} browsed at ${right.time} and bought at ${left.time}") //diff must be < 2sc
        }
      })

    intervalJoinStream.print()
    env.execute()
  }

  // CONNECT
  // two streams are treated with the same operator
  def demoConnect(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setMaxParallelism(1)
    val shoppingCartEvents = env.addSource(new SingleShoppingCartEventsGenerator(100, sourceId = Option("kafka"))).setParallelism(1)
    val catalogEvents = env.addSource(new CatalogEventsGenerator(1000)).setParallelism(1)
    val connectedStreams: ConnectedStreams[ShoppingCartEvent, CatalogEvent] = shoppingCartEvents.connect(catalogEvents)

    //vars // single thread // 2 diff types at the same type
    //ratio
    val ratioString: DataStream[Double] = connectedStreams.process(
      new CoProcessFunction[ShoppingCartEvent, CatalogEvent, Double] {
        var shoppingCartEventCount = 0
        var catalogEventCount = 0

        override def processElement1(
                                      value: ShoppingCartEvent,
                                      ctx: CoProcessFunction[ShoppingCartEvent, CatalogEvent, Double]#Context,
                                      out: Collector[Double]): Unit = {
          shoppingCartEventCount += 1
          out.collect(shoppingCartEventCount * 100.0 / (shoppingCartEventCount + catalogEventCount))
        }

        override def processElement2(value: CatalogEvent,
                                     ctx: CoProcessFunction[ShoppingCartEvent, CatalogEvent, Double]#Context,
                                     out: Collector[Double]): Unit = {
          catalogEventCount += 1
          out.collect(catalogEventCount * 100.0 / (catalogEventCount + shoppingCartEventCount))
        }
      }
    )
    ratioString.print()
    env.execute()
  }


  def main(args: Array[String]): Unit = {
    demoConnect()
  }

}
