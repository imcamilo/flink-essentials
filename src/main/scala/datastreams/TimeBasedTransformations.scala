package datastreams

import generators.gaming.{PlayerRegistered, ServerEvent}
import generators.shopping._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, Watermark, WatermarkGenerator, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Instant

/*
* EVENT TIME - CREATED AT THE SOURCE
* PROCESSING TIME - ARRIVED TO FLINK
* */
object TimeBasedTransformations {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  private val shoppingCartEvents: DataStream[ShoppingCartEvent] = env.addSource(new ShoppingCartEventsGenerator(
    sleepMillisPerEvent = 100,
    batchSize = 5,
    baseInstant = Instant.parse("2022-02-15T00:00:00.000Z")
  ))

  // We have to extract it if we wanna process them using this semantic
  // 1. Using the Event Time - The moment the event was created
  // 2. Using the Processing Time - The moment the event arrives to Flink.

  /*
  * Group by window every 3 seconds
  * Tumbling, non-overlapping
  * Processing Time
  * */

  //1. PROCESSING TIME
  private class CountByWindowAll extends ProcessAllWindowFunction[ShoppingCartEvent, String, TimeWindow] {
    override def process(context: Context, elements: Iterable[ShoppingCartEvent], out: Collector[String]): Unit = {
      val window = context.window
      // every single window will produce a single string down-stream which its contains the number of elements grouped
      out.collect(s"Window [${window.getStart}-${window.getEnd}] ${elements.size}")
    }
  }

  private def demoProcessingTime(): Unit = {
    val groupedEventsByWindows = shoppingCartEvents.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(3)))

    def countEventsByWindows: DataStream[String] = groupedEventsByWindows.process(new CountByWindowAll)

    countEventsByWindows.print()
    env.execute()
  }

  /*
  With processing time:
  - We dont care when the event was created
  - Multiple runs generate different results
  */

  //2. EVENT TIME
  private def demoEventTime(): Unit = {
    //watermark, strong but clunky api
    val tsAssigner = new SerializableTimestampAssigner[ShoppingCartEvent] {
      override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long = {
        element.time.toEpochMilli
      }
    }
    val timeInWatermark = java.time.Duration.ofMillis(500) //max delay must be less than 500 millis
    val groupedEventsByWindows = shoppingCartEvents
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness(timeInWatermark)
          .withTimestampAssigner(tsAssigner) //given an element, youll returna milli scond count
      ).windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))

    val countEventsByWindow = groupedEventsByWindows.process(new CountByWindowAll)
    countEventsByWindow.print()
    env.execute()
  }
  /*
   With event time:
   - We need to care about handle late data - Done with Watermarks
   - I dont care Flink internal time
   - We might see faster results
   - See same events + different runs = same results
   */


  /*CUSTOM WATERMARKS*/

  // WITH every new timestamp, every new incoming element with event time < new timestamp - max delay (will be discarded)
  class BoundedOutOfOrdernessGenerator(maxDelay: Long) extends WatermarkGenerator[ShoppingCartEvent] {
    private var currentMaxTimestamp: Long = 0L

    //will be invoked when the data stream processing a new event. Every single time, this will be called
    override def onEvent(event: ShoppingCartEvent, eventTimestamp: Long, output: WatermarkOutput): Unit = {
      //                  ^ event beign process   ^ ts attached to event  ^
      //                                                                    immutable structure that will allow
      //                                                                    u to push a new watermark to flink handle later
      currentMaxTimestamp = Math.max(currentMaxTimestamp, event.time.toEpochMilli)
      //import watermark from from eventtime
      //every new event older than this event will be discarded
      //emitting watermark is not mandatory
      output.emitWatermark(new Watermark(event.time.toEpochMilli))
    }

    // flink also call onPeriodicEmit to MAYBE emit watermarks regularly
    // flink also call onPeriodicEmit regulary - up to us emit maybe emit a watermark at these times
    override def onPeriodicEmit(output: WatermarkOutput): Unit = {
      output.emitWatermark(new Watermark(currentMaxTimestamp - maxDelay - 1))
    }
  }

  private def demoEventTimeV2(): Unit = {

    // control how often Flink calls on periodicEmit
    env.getConfig.setAutoWatermarkInterval(1000L) //call periodicEmit every 1 second

    //watermark, strong but clunky api
    val tsAssigner = new SerializableTimestampAssigner[ShoppingCartEvent] {
      override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long = {
        element.time.toEpochMilli
      }
    }
    val groupedEventsByWindows = shoppingCartEvents
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forGenerator(_ => new BoundedOutOfOrdernessGenerator(500L))
          .withTimestampAssigner(tsAssigner) //given an element, youll returna milli scond count
      ).windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))

    val countEventsByWindow = groupedEventsByWindows.process(new CountByWindowAll)
    countEventsByWindow.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    //demoProcessingTime()
    demoEventTimeV2()
  }

}
