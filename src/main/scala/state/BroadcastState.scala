package state

import generators.shopping._
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object BroadcastState {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val shoppingCartEvents: DataStream[ShoppingCartEvent] = env.addSource(new SingleShoppingCartEventsGenerator(100))
  val eventsByUser: KeyedStream[ShoppingCartEvent, String] = shoppingCartEvents.keyBy(_.userId)

  // issue: if the qty is > threshold
  def purchaseWarnings(): Unit = {
    val threshold = 2
    val notificationStream = eventsByUser
      .filter(_.isInstanceOf[AddToShoppingCartEvent])
      .filter(_.asInstanceOf[AddToShoppingCartEvent].quantity > threshold)
      .map(event => event match {
        case AddToShoppingCartEvent(userId, sku, quantity, time) =>
          s"User $userId attempting to purchase $quantity items of $sku when threshold is $threshold"
        case _ => ""
      })
    // what happens if threshold changes over time
    //the threshold must be broadcasted and it will


    notificationStream.print()
    env.execute()
  }

  def changingThreshold(): Unit = {
    val thresholds: DataStream[Int] = env.addSource(new SourceFunction[Int] { //something than emits integers
      override def run(ctx: SourceFunction.SourceContext[Int]): Unit =
        List(2, 0, 4, 5, 6, 3).foreach { newThreshold =>
          Thread.sleep(1000)
          ctx.collect(newThreshold)
        }

      override def cancel(): Unit = ???
    })

    //broadcast state is always a MAP
    val broadcastStateDescriptor: MapStateDescriptor[String, Int] =
      new MapStateDescriptor[String, Int]("thresholds", classOf[String], classOf[Int])
    val broadcastThreshold: BroadcastStream[Int] = thresholds.broadcast(broadcastStateDescriptor)

    val notificationStreams = eventsByUser
      .connect(broadcastThreshold)
      .process(new KeyedBroadcastProcessFunction[String, ShoppingCartEvent, Int, String] {
        //                                       ^key    ^first event      ^broadcast ^output

        // this will be executed on worker node
        val thresholdDescriptor = new MapStateDescriptor[String, Int]("thresholds", classOf[String], classOf[Int])

        override def processBroadcastElement(newThreshold: Int,
                                             ctx: KeyedBroadcastProcessFunction[String, ShoppingCartEvent, Int, String]#Context,
                                             out: Collector[String]): Unit = {
          println(s"Threshold about to be changed - $newThreshold")
          //fetch the broadcast event - distributed variable
          val stateThresholds = ctx.getBroadcastState(thresholdDescriptor)
          //update state, just like a map
          stateThresholds.put("quantity-threshold", newThreshold)
        }

        override def processElement(event: ShoppingCartEvent,
                                    ctx: KeyedBroadcastProcessFunction[String, ShoppingCartEvent, Int, String]#ReadOnlyContext,
                                    out: Collector[String]): Unit = {
          event match {
            case AddToShoppingCartEvent(userId, sku, quantity, time) =>
              val currentThreshold: Int = ctx.getBroadcastState(thresholdDescriptor).get("quantity-threshold")
              if (quantity > currentThreshold)
                out.collect(s"User $userId attempting to purchase $quantity items of $sku when threshold is $currentThreshold"
                )
          }
        }

      })

    notificationStreams.print()
    env.execute()

  }


  def main(args: Array[String]): Unit = {
    changingThreshold()
  }

}
