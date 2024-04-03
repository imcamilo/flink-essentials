package state

import generators.shopping._
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.{CheckpointListener, ValueState, ValueStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/*
* Checkpoint = Store the entire state at an exact point in time
* Distributed system -> inherently unreliable
* Need to deal with failures:
*   Kill the process
*   Failed/Unreachable machines
*
* Naive
* - pause the app and data ingestion
* - wait for the current data to be processed
* - copy the state to the backend
* - resume data ingestion after that...
* ... increasing latency.. u cant stop the app
*
* Flink solution:
* - stream is running
* - the JobManager adds a checkpoint barrier (special piece of data)
* - Task saves its state and forwards the barrier
* - Barrier arrives at Task, they save their state
* - if new elements arrive while saving, buffer
* - barrier moves forward, element continue to flow
* - the sinks ACK the checkpoint to JobManager
* - checkpoint complete, move ons
*
* */
object Checkpoints {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  // set checkpoint interval
  env.getCheckpointConfig.setCheckpointInterval(5000) // a checkpoint triggered every 5s
  // set checkpoint storage
  env.getCheckpointConfig.setCheckpointStorage("file:///Users/imcamilo/Projects/flink/checkpoints")

  /*
  * Keep track the number of AddCart events per User
  * when quantity is bigger than a threshold (e.g. managing stock)
  * Persist the data (state) via checkpoints
  * */

  val shoppingCartEvents: DataStream[ShoppingCartEvent] =
    env.addSource(new SingleShoppingCartEventsGenerator(sleepMillisBetweenEvents = 100, generateRemoved = true))

  private val eventsByUser = shoppingCartEvents
    .keyBy(_.userId)
    .flatMap(new HighQuantityCheckpointedFunction(5))

  def main(args: Array[String]): Unit = {
    eventsByUser.print()
    env.execute()
  }

}

class HighQuantityCheckpointedFunction(val threshold: Long)
  extends FlatMapFunction[ShoppingCartEvent, (String, Long)]
    with CheckpointedFunction
    with CheckpointListener {

  var stateCount: ValueState[Long] = _ // instantiated per key

  override def flatMap(event: ShoppingCartEvent, out: Collector[(String, Long)]): Unit = {
    event match {
      case AddToShoppingCartEvent(userId, _, quantity, _) =>
        if (quantity < threshold) {
          //update state
          val newUserEventCount = stateCount.value() + 1
          stateCount.update(newUserEventCount)

          //push output
          out.collect((userId, newUserEventCount))
        }
      case _ => //
    }
  }

  //  CheckpointedFunction
  // its been invoke when the checkpoint is triggered
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    println(s"Checkpoint at: ${context.getCheckpointTimestamp}")
  }

  // life cycle method to initialize state (-open() in rich functions)
  override def initializeState(context: FunctionInitializationContext): Unit = {
    val stateCountDescriptor = new ValueStateDescriptor[Long]("impossible-order-count", classOf[Long])
    stateCount = context.getKeyedStateStore.getState(stateCountDescriptor)
  }
  //  CheckpointedFunction

  //  CheckpointListener
  override def notifyCheckpointComplete(checkpointId: Long): Unit = ()

  //optional
  override def notifyCheckpointAborted(checkpointId: Long): Unit = ()
  //  CheckpointListener

}