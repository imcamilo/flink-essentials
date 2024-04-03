package state

import generators.shopping._
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object KeyedState {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  private val shoppingCartEventStream: DataStream[ShoppingCartEvent] =
    env.addSource(new SingleShoppingCartEventsGenerator(
      sleepMillisBetweenEvents = 100, //10 events/sec
      generateRemoved = true))
  private val eventsPerUser: KeyedStream[ShoppingCartEvent, String] = shoppingCartEventStream.keyBy(_.userId)

  /*
  In practice operator state its very rarely to used
  There are very few use cases that justify keeping states separate for a particular operator task.
  ...each task will have access to its own state, in a multi threading environment
  */
  // Value State -> keeping just a value
  private def demoValueState(): Unit = {
    /*
    How many events per users have been generated?
     */

    val numEventsPerUserNaive = eventsPerUser.process(
      new KeyedProcessFunction[String, ShoppingCartEvent, String] { //instantiate ONCE per key
        //                     ^ key   ^ event            ^ result
        var nEventsPerThisUser = 0

        def processElement(value: ShoppingCartEvent,
                           ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
                           out: Collector[String]): Unit = {
          nEventsPerThisUser += 1
          out.collect(s"User ${value.userId} - $nEventsPerThisUser")
        }
      })

    /*Local with local vars
    * - they are local, so other nodes dont see them
    * - if the node crashes, the var disappears*/

    val numEventsPerUsersStream = eventsPerUser.process(
      new KeyedProcessFunction[String, ShoppingCartEvent, String] {

        //SOLID, starts with 'state'
        // then you can call .value to get the current state
        // then you can call .update(newValue) to overwrite
        var stateCounter: ValueState[Long] = _ // a value state per key -> userId

        override def open(parameters: Configuration): Unit = {
          //initialize state
          stateCounter = getRuntimeContext // method from RichContext
            .getState(new ValueStateDescriptor[Long]("events-counter", classOf[Long]))
        }

        override def processElement(value: ShoppingCartEvent,
                                    ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
                                    out: Collector[String]): Unit = {
          val nEventsPerThisUser = stateCounter.value()
          stateCounter.update(nEventsPerThisUser + 1L)
          out.collect(s"User ${value.userId} - ${nEventsPerThisUser + 1}")

        }
      }
    )

    numEventsPerUsersStream.print()
    env.execute()
  }

  // ListState -> keeping the events
  private def demoListState(): Unit = {
    // store all the events per user id
    val allEventsPerUserStream = eventsPerUser.process(
      new KeyedProcessFunction[String, ShoppingCartEvent, String] {
        //create state here
        /*
        Capabilities:
        - add(value)
        - addAll(list)
        - update(new List) overwritting
        - get
        - by deafult we dont have delete values in that case, do the update with a new list
        */
        var stateEventsForUser: ListState[ShoppingCartEvent] = _
        // you need to be careful to keep the size of the list bounded. Dont crash your worker machine

        //init
        override def open(parameters: Configuration): Unit = {
          stateEventsForUser = getRuntimeContext
            .getListState(new ListStateDescriptor[ShoppingCartEvent]("shopping-cart-event", classOf[ShoppingCartEvent]))
        }

        override def processElement(event: ShoppingCartEvent,
                                    ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
                                    out: Collector[String]): Unit = {
          stateEventsForUser.add(event)

          // implicit converters(extension methods)
          import scala.collection.JavaConverters._ // 2.12
          //import scala.jdk.CollectionConverters._ // 3
          val currentEvents: Iterable[ShoppingCartEvent] = stateEventsForUser.get() // java iterable
            .asScala // to scala iterable
          out.collect(s"User ${event.userId} - ${currentEvents.mkString(", ")}") // single item out through the collector
        }

      }
    )
    allEventsPerUserStream.print()
    env.execute()
  }

  // ListState2 -> keeping the events and clearing
  private def demoListStateWithClearance(): Unit = {
    val allEventsPerUserStream = eventsPerUser.process(
      new KeyedProcessFunction[String, ShoppingCartEvent, String] {

        import scala.collection.JavaConverters._

        //limit to 10, when we have 10 elements, clear the cache
        var stateEventsForUser: ListState[ShoppingCartEvent] = _

        override def open(parameters: Configuration): Unit = {
          val descriptor = new ListStateDescriptor[ShoppingCartEvent]("shopping-cart-event", classOf[ShoppingCartEvent])
          //TTL: time to live = cleared if its not modified after a certain time.
          descriptor.enableTimeToLive(
            StateTtlConfig
              .newBuilder(Time.hours(1)) //Clear after 1 hour
              .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) //Specify when the timer resets
              .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
              .build()
          )
          stateEventsForUser = getRuntimeContext.getListState(descriptor)

        }

        override def processElement(event: ShoppingCartEvent,
                                    ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
                                    out: Collector[String]): Unit = {
          stateEventsForUser.add(event)
          val currentEvents = stateEventsForUser.get.asScala.toList
          if (currentEvents.size > 10)
            stateEventsForUser.clear() // clear is not done immediately

          out.collect(s"User ${event.userId} - ${currentEvents.mkString(", ")}") // single item out through the collector
        }

      }
    )
    allEventsPerUserStream.print()
    env.execute()
  }

  // MapState
  private def demoMapState(): Unit = {
    // count how many events PER TYPE were ingested PER USER
    val streamCountPerType = eventsPerUser.process(
      new KeyedProcessFunction[String, ShoppingCartEvent, String] {
        //create a state
        var stateCountPerEventType: MapState[String, Long] = _

        //init the state here
        override def open(parameters: Configuration): Unit = {
          stateCountPerEventType = getRuntimeContext.getMapState(
            new MapStateDescriptor[String, Long]("per-type-counter", classOf[String], classOf[Long]) //two types (map)
          )
        }

        import scala.collection.JavaConverters._ // 2.12

        //the map state provides the common map methods
        override def processElement(event: ShoppingCartEvent,
                                    ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
                                    out: Collector[String]): Unit = {
          //fetch the type for the event
          val eventType = event.getClass.getSimpleName
          if (stateCountPerEventType.contains(eventType)) {
            //update and increase counter
            val oldCount = stateCountPerEventType.get(eventType)
            val newCount = oldCount + 1
            stateCountPerEventType.put(eventType, newCount)
          } else {
            stateCountPerEventType.put(eventType, 1)
          }

          //push some output
          out.collect(s"${ctx.getCurrentKey} - ${stateCountPerEventType.entries().asScala.mkString(", ")}")
        }
      }
    )
    streamCountPerType.print()
    env.execute()
  }

  /*
  * You can clear the state manually
  * You can clear the state at a regular interval
  * */
  def main(args: Array[String]): Unit = {
    demoListStateWithClearance()
  }

}
