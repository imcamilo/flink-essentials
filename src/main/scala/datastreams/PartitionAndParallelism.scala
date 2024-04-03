package datastreams

import generators.shopping._
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._

object PartitionAndParallelism {

  // Splitting = Partitioning

  private def demoPartitioner(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val shoppingCartEvents: DataStream[ShoppingCartEvent] =
      env.addSource(new SingleShoppingCartEventsGenerator(100)) //10 events/sec
    // partition = logic to split the data. The type is the key by u want to split it
    val partitioner = new Partitioner[String] {
      //num partition would be the number of cores in ur pc
      override def partition(key: String, numPartitions: Int): Int = { //invoke on every event
        //hash code % number of partitions - even distribution
        val valid = key.hashCode % numPartitions
        println(s"Number of max partitions: $numPartitions - partitioning by: ${valid}")
        valid
      }
    }

    val partitionedStream = shoppingCartEvents.partitionCustom(
      partitioner,
      event => event.userId
    )

    /*
     bad bcs
     - loose parallelism
     - your risk to overloading the task with disproportionate data

     Good for e.g. sending http requests
     */
    val badPartitioner: Partitioner[String] = new Partitioner[String] {
      override def partition(key: String, numPartitions: Int): Int = {
        // last partition index
        numPartitions - 1
      }
    }

    val badPartitionedStream = shoppingCartEvents.partitionCustom(
        badPartitioner,
        event => event.userId
      )
      .shuffle // Shuffle redistribute data evenly - involves data transfer through network, so its expensive.

    //partitionedStream.print()

    badPartitionedStream.print()
    env.execute()

  }

  def main(args: Array[String]): Unit = {
    demoPartitioner()
  }
}
