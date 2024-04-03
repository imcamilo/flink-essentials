package io

import generators.shopping
import generators.shopping.{ShoppingCartEvent, SingleShoppingCartEventsGenerator}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputs {

  // shopping cart events
  // process in 2 diff ways with the same function
  // e.g. events for user Alice, and  all the events of everyone else

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val shoppingCartEvents: DataStream[shopping.ShoppingCartEvent] =
    env.addSource(new SingleShoppingCartEventsGenerator(100))

  // simple data structure, push shopping cart events through some output. Name should be unique
  val aliceTag = new OutputTag[ShoppingCartEvent]("alice-events")

  // output tags - only available for ProcessFunction
  class AliceEventsFunction extends ProcessFunction[ShoppingCartEvent, ShoppingCartEvent] {

    override def processElement(
                                 event: ShoppingCartEvent,
                                 ctx: ProcessFunction[ShoppingCartEvent, ShoppingCartEvent]#Context,
                                 out: Collector[ShoppingCartEvent] // "primary" destination
                               ): Unit = {
      if (event.userId.equals("Alice")) {
        ctx.output(aliceTag, event) // collecting an event through a secondary destination
      } else {
        out.collect(event)
      }
    }

  }

  def demoSideOutput(): Unit = {
    val allEventsButAlice: DataStream[ShoppingCartEvent] = shoppingCartEvents.process(new AliceEventsFunction)
    //check side outputs
    val aliceEvents: DataStream[ShoppingCartEvent] = allEventsButAlice.getSideOutput(aliceTag)
    // process the data streams separately
    allEventsButAlice.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    demoSideOutput()
  }

}
