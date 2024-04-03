package datastreams

import generators.gaming.{PlayerRegistered, ServerEvent}
import generators.shopping._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
* when window function is ready to executed
* */
object Triggers {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  private def demoCountTrigger(): Unit = {
    val shoppingCartEvents: DataStream[String] = env
      .addSource(new ShoppingCartEventsGenerator(sleepMillisPerEvent = 500, batchSize = 2)) // 2 events per second
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))) //10 events per window
      //tumbling, is executed when window finishes, trigger modifies that.
      //window function runs every five elements
      .trigger(CountTrigger.of[TimeWindow](5))
      .process(new CountByWindowAll) // DataStream[String]
    shoppingCartEvents.print()
    env.execute()
  }

  // purging trigger
  private def demoPurgingTrigger(): Unit = {
    val shoppingCartEvents: DataStream[String] = env
      .addSource(new ShoppingCartEventsGenerator(sleepMillisPerEvent = 500, batchSize = 2)) // 2 events per second
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))) //10 events per window
      //window function runs every five elements, then clears the window
      .trigger(PurgingTrigger.of(CountTrigger.of[TimeWindow](5)))
      .process(new CountByWindowAll) // DataStream[String]
    shoppingCartEvents.print()
    env.execute()
  }

  /*Other triggers
  *
  * - EventTimeTrigger - happens by default when a watermark is larger than window end time (automatic for event time windows)
  * - ProcessingTimeTrigger - fires when the current system time is bigger than window end time (automatic for processing time windows)
  * - CustomTriggers - powerful API for custom firing behavior
  * */


  def main(args: Array[String]): Unit = {
    demoPurgingTrigger()
  }

  //copied from time based transformations
  private class CountByWindowAll extends ProcessAllWindowFunction[ShoppingCartEvent, String, TimeWindow] {
    //                                                            ^input             ^output ^window type

    override def process(context: Context, elements: Iterable[ShoppingCartEvent], out: Collector[String]): Unit = {
      val window = context.window
      out.collect(s"Window [${window.getStart}-${window.getEnd}] ${elements.size}")
    }
  }

}
