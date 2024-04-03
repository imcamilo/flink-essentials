package datastreams

import generators.gaming._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.time.Instant
import scala.concurrent.duration._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, GlobalWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

import java.time

/*
  use case => stream of events for a gaming session
*/

object WindowsFunctions {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  implicit val serverStartTime: Instant = Instant.parse("2022-02-02T00:00:00.000Z")


  // TUMBLING WINDOWS
  // dont overlap

  val events: List[ServerEvent] = List(
    bob.register(2.seconds), // player "Bob" register 2s after the server started
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

  // how many players were register, every 3 seconds
  // [0s...3s] [3s...6s] [6s...9s]

  private val serializerTimestampAssigner: SerializableTimestampAssigner[ServerEvent] = new SerializableTimestampAssigner[ServerEvent] {
    //recordTimestamp is the time when flink received the element
    override def extractTimestamp(element: ServerEvent, recordTimestamp: Long): Long = element.eventTime.toEpochMilli
  }
  //element.eventTime.toEpochMilli
  //how to extract timestamp for events + watermarks (always define it)
  //once you get an event with time T, you will NOT accept further events with time < T - 500

  private val eventStream: DataStream[ServerEvent] =
    env.fromCollection(events)
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofMillis(5000))
          .withTimestampAssigner(serializerTimestampAssigner)
      )

  // 1. group by window
  private val windowDuration = TumblingEventTimeWindows.of(Time.seconds(3))
  private val threeSecondsTumblingWindow = eventStream.windowAll(windowDuration)

  /*
  *
  * ----- window 1 ----- 1 registration
  * 0s  -   []
  * 1s  -   []
  * 2s  -   [bob registered, bob online]
  * ----- window 2 ----- 3 registration
  * 3s  -   [sam registered]
  * 4s  -   [sam online, rob registered, alice registered]
  * 5s  -   []
  * ----- window 3 ----- 2 registration
  * 6s  -   [mary registered, mary online]
  * 7s  -   []
  * 8s  -   [carl registered]
  * ----- window 4 ----- 0 registration
  * 9s  -   []
  * 10  -   [rob online, alice online, carl online]
  * 11  -   []
  *
  */

  // count by windowAll

  //V1
  private class CountByWindowAll extends AllWindowFunction[ServerEvent, String, TimeWindow] {
    //                                                       ^input       ^output ^window type
    override def apply(window: TimeWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val registrationEventCount = input.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [${window.getStart}-${window.getEnd}] $registrationEventCount")
    }
  }

  private def demoCountByWindow(): Unit = {
    //apply uses an AllWindowFunction
    val registrationsPerThreeSeconds: DataStream[String] = threeSecondsTumblingWindow.apply(new CountByWindowAll)
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  //V2. Context its richer. Process window function which offers a much richer API (lower-level)
  private class CountByWindowAllV2 extends ProcessAllWindowFunction[ServerEvent, String, TimeWindow] {
    override def process(context: Context, elements: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val window = context.window
      val registrationEventCount = elements.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [${window.getStart}-${window.getEnd}] $registrationEventCount")
    }
  }

  private def demoCountByWindow2(): Unit = {
    //apply uses an ProcessAllWindowFunction
    val registrationsPerThreeSeconds: DataStream[String] = threeSecondsTumblingWindow.process(new CountByWindowAllV2)
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  //V3. Aggregate Function
  private class CountByWindowVersion3 extends AggregateFunction[ServerEvent, Long, Long] {
    //                                                  ^ input      ^ acc ^ output
    override def createAccumulator(): Long = 0L //start counting from 0

    //every element increases acc by 1
    override def add(value: ServerEvent, accumulator: Long): Long =
      if (value.isInstanceOf[PlayerRegistered]) accumulator + 1
      else accumulator

    // push the final output out of the final accumulator
    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  private def demoCountByWindow3(): Unit = {
    //apply uses an ProcessAllWindowFunction
    val registrationsPerThreeSeconds: DataStream[Long] = threeSecondsTumblingWindow.aggregate(new CountByWindowVersion3)
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  /*
    KStreams And Window Functions
    types [original type, key for every event]
    keyBy, stream its been split
    each element will be assigned to a "mini stream" for its own key
  */
  val streamByType: KeyedStream[ServerEvent, String] = eventStream.keyBy(e => e.getClass.getSimpleName) //just extract the class name
  /*
    now we can run a window function
    3 seconds tumbling windows - window no windowAll
    for every key (based on the class name), we will have a separate window allocation

    so... for registration key:

    in the first window [0s ... 3s] we will have 1 registration
    in the second window [3s ... 6s] we will have 3 registration
    in the third window [6s ... 9s] we will have 2 registration

    so... for online key:

    in the first window [0s ... 3s] we will have 1 online
    in the second window [3s ... 6s] we will have 1 online
    in the third window [6s ... 9s] we will have 1 online

    etc..
  */
  private val threeSecTumblingWindowByType = streamByType.window(windowDuration)

  /*
  The last windows all functions take 3 types arguments: Event Type, Output Type, Window Type.
  In this WindowFunction we will have 4 type arguments: Event Type, Output Type, Key Type, Window Type

  and window function online have one method to implement
   */
  class CountByWindow extends WindowFunction[ServerEvent, String, String, TimeWindow] {
    // for every key
    // for every window
    // given an input (secuence of event types)
    // we have a collector to push result downstream
    override def apply(key: String, window: TimeWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit = {
      out.collect(s"$key $window ${input.size}")
    }
  }

  def demoCountByTypeByWindow(): Unit = {
    val finalStream = threeSecTumblingWindowByType.apply(new CountByWindow)
    finalStream.print()
    env.execute()
  }

  // alternative with process
  class CountByWindow2 extends ProcessWindowFunction[ServerEvent, String, String, TimeWindow] {
    //
    override def process(key: String, context: Context, elements: Iterable[ServerEvent], out: Collector[String]): Unit = {
      out.collect(s"$key ${context.window} ${elements.size}")
    }
  }

  def demoCountByTypeByWindow2(): Unit = {
    val finalStream = threeSecTumblingWindowByType.process(new CountByWindow2)
    finalStream.print()
    env.execute()
  }

  // TASK
  /*
  One task processed all the data related to a particular key
   */

  // SLIDING WINDOWS
  /* they overlap

   how many players were register, every 3 seconds. UPDATED every second.
    [0s...3s] [1s...4s] [2s...5s]
  */

  private def demoSlidingAllWindow() = {

    val windowDuration: Time = Time.seconds(3)
    val slidingTime: Time = Time.seconds(1)
    //splitting event stream into windows
    val slidingWindowsAll = eventStream.windowAll(SlidingEventTimeWindows.of(windowDuration, slidingTime))
    // process data and stream similar to the window function.
    // I could pick any window function already created...
    val registrationCountByWindow = slidingWindowsAll.apply(new CountByWindowAll)

    //similar
    registrationCountByWindow.print()
    env.execute
  }

  // SESSION WINDOWS
  /* group of events with NO MORE THAN a certain time gap in between them

   are powerful bcs it allows gruop of elements in not equals windows.

   how many registration events do we have, NO MORE THAN a 1 second APART?
    by instance:
      if a key is detected, then we started to receive events, but if happens 1 second without events the session is closed.
      and a new session will start again when a new event with the same key comes from the source.
      Session window, are not necessary equal.
  */

  private def demoSessionWindow(): Unit = {
    val groupBySessionWindow = eventStream.windowAll(EventTimeSessionWindows.withGap(Time.seconds(1)))
    //operate any kind of window function
    val countBySessionWindow = groupBySessionWindow.apply(new CountByWindowAll)
    // you can do the same things as before
    countBySessionWindow.print()
    env.execute()
  }

  // GLOBAL WINDOWS
  /* One giant window

   how many registration events do we have every 10 events?
  */

  //V1
  private class CountByGlobalWindowAll extends AllWindowFunction[ServerEvent, String, GlobalWindow] {
    //                                                       ^input       ^output ^window type
    override def apply(window: GlobalWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val registrationEventCount = input.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [$window] $registrationEventCount")
    }
  }

  def demoGlobalWindow(): Unit = {
    // this will handle all the events so we have to define a kind of finiteness
    // otherwise Flink aggregate data indefinitely, so we have to specify ...every 10 events for instance

    val globalWindowEvents = eventStream
      .windowAll(GlobalWindows.create())
      .trigger(CountTrigger.of[GlobalWindow](10)) //each 10 elements
      .apply(new CountByGlobalWindowAll) //but its not compatible with the window all, bcs the time its different

    globalWindowEvents.print()

    env.execute()
  }

  /*
  * EXERCISE
  * What was the time window (continuous 2s) when we had the most number of registration events
  * (I gonna return the value (agg) of the window itself b.e. [window1: 1. window2: 1, window3: 3] )
  * if we want to return the actual values beign obtain after a stream, we can say
  * eventStream.executeAndCollect() - returning the actual values
  *
  * what kind of window function should we use? ALL WINDOW FUNCTION
  * what kind of window should we use? SLIDING windows
  *
  * */
  //we wanna keep the window itself
  class KeepWindowAndCountFunction extends AllWindowFunction[ServerEvent, (TimeWindow, Long), TimeWindow] {
    override def apply(window: TimeWindow, input: Iterable[ServerEvent], out: Collector[(TimeWindow, Long)]): Unit = {
      out.collect((window, input.size)) //pushing tuple outside stream
    }
  }

  def windowFunctionExercise(): Unit = {
    // only care about registration events
    val sliddingWindows: DataStream[(TimeWindow, Long)] = eventStream
      .filter(_.isInstanceOf[PlayerRegistered])
      .windowAll(SlidingEventTimeWindows.of(
        Time.seconds(2),
        Time.seconds(1)
      ))
      .apply(new KeepWindowAndCountFunction)
    //we have to process the stream with a count by window
    val localWindows: List[(TimeWindow, Long)] = sliddingWindows.executeAndCollect().toList
    val bestWindow: (TimeWindow, Long) = localWindows.maxBy(_._2)
    println(s"The best window is ${bestWindow._1} with ${bestWindow._2} registration events")
  }


  def main(args: Array[String]): Unit = {
    //tumblings
    //demoCountByWindow()
    //demoCountByWindow2()
    //demoCountByWindow3()
    //demoCountByTypeByWindow()
    //demoCountByTypeByWindow2() // events associate to the particular key, will be the same thread and same machine
    //slidings
    demoSlidingAllWindow()
    //sessions
    //demoSessionWindow()
    //global
    //demoGlobalWindow()
    //collecting
    //windowFunctionExercise()
  }


}
