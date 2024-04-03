package io

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

import java.io.PrintStream
import java.net.{ServerSocket, Socket}
import java.util.Scanner
import scala.util.Random

object CustomSources {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  // source of numbers randomly generated/ Single thread
  private class RandomGeneratorSource(minEventsPerSecond: Double) extends SourceFunction[Long] {

    // we can create local field/methods
    private val maxSleepTime: Long = (1000 / minEventsPerSecond).toLong

    @volatile
    private var isRunning: Boolean = true // instead mutability - cats effects / ZIO

    // only called once, when ds is instantiated
    // this runs in a dedicated thread
    override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
      while (isRunning) {
        val sleepTime = Math.abs(Random.nextLong() % maxSleepTime)
        val nextNumber = Random.nextLong()
        Thread.sleep(sleepTime)
        //push something to the output
        ctx.collect(nextNumber)
      }
    }

    // called when app is shutdown
    // contract: run method should stop immediately
    override def cancel(): Unit =
      isRunning = false
  }

  //offers state and capability of overwrite lifecycle methods
  private class RichRandomGeneratorSource(minEventsPerSecond: Double) extends RichSourceFunction[Long] {

    // we can create local field/methods
    private val maxSleepTime: Long = (1000 / minEventsPerSecond).toLong

    @volatile
    private var isRunning: Boolean = true // instead mutability - cats effects / ZIO

    // only called once, when ds is instantiated
    // this runs in a dedicated thread
    override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
      while (isRunning) {
        val sleepTime = Math.abs(Random.nextLong() % maxSleepTime)
        val nextNumber = Random.nextLong()
        Thread.sleep(sleepTime)
        //push something to the output
        ctx.collect(nextNumber)
      }
    }

    // called when app is shutdown
    // contract: run method should stop immediately
    override def cancel(): Unit = isRunning = false

    // capability of lifecycle methods - initialize state
    override def open(parameters: Configuration): Unit =
      println(s"${Thread.currentThread().getName} starting source function")

    override def close(): Unit =
      println(s"${Thread.currentThread().getName} closing source function")

    //can hold state - ValueState - ListState - MapState
  }

  private class RichParallelRandomGeneratorSource(minEventsPerSecond: Double)
    extends RichParallelSourceFunction[Long] {

    // we can create local field/methods
    private val maxSleepTime: Long = (1000 / minEventsPerSecond).toLong

    @volatile
    private var isRunning: Boolean = true // instead mutability - cats effects / ZIO

    // ITS CALLED ONCE PER THREAD, each instance has its own thread
    override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
      while (isRunning) {
        val sleepTime = Math.abs(Random.nextLong() % maxSleepTime)
        val nextNumber = Random.nextLong()
        Thread.sleep(sleepTime)
        //push something to the output
        ctx.collect(nextNumber)
      }
    }

    // called when app is shutdown
    // contract: run method should stop immediately
    override def cancel(): Unit = isRunning = false

    // capability of lifecycle methods - initialize state
    override def open(parameters: Configuration): Unit =
      println(s"${Thread.currentThread().getName} starting source function")

    override def close(): Unit =
      println(s"${Thread.currentThread().getName} closing source function")

    //can hold state - ValueState - ListState - MapState
  }

  /*
    create a source function that reads data from sockets
   */
  private class SocketStringSource(host: String, port: Int) extends RichSourceFunction[String] {
    //resource. Whenever you manage a resource, use a RichSourceFunction
    private var socket: Socket = _
    private var isRunning: Boolean = true

    override def open(parameters: Configuration): Unit = {
      socket = new Socket(host, port)
    }

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      val scanner = new Scanner(socket.getInputStream)
      while (isRunning && scanner.hasNextLine) {
        ctx.collect(scanner.nextLine())
      }
    }

    override def cancel(): Unit = {
      isRunning = false
    }

    override def close(): Unit = {
      socket.close()
    }

  }

  private def demoSocketSource(): Unit = {
    val socketStringStream = env.addSource(new SocketStringSource("localhost", 12345))
    socketStringStream.print()
    env.execute()
  }

  private def demoSourceFunction(): Unit = {
    // env.addSource(new RandomGeneratorSource(10))
    // env.addSource(new RichRandomGeneratorSource(10))
    val numbersStream: DataStream[Long] =
      env.addSource(new RichParallelRandomGeneratorSource(10)).setParallelism(10)
    numbersStream.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    //demoSourceFunction()
    demoSocketSource()
  }

}

/*
* - start data sender
* - start flink
* - data sender => Flink
* */
object DataSender {
  def main(args: Array[String]): Unit = {
    val serverSocket = new ServerSocket(12345)
    println("Waiting for Flink to connect...")

    val socket = serverSocket.accept()
    println("Flink connected. Sending data...")

    val printer = new PrintStream(socket.getOutputStream)
    printer.println("Hello from the other side...")
    Thread.sleep(3000)
    printer.println("Almost ready...")
    Thread.sleep(5000)
    (1 to 10).foreach { i =>
      Thread.sleep(200)
      printer.println(s"Number $i")
    }
    println("Data sending completed.")
    serverSocket.close()
  }
}
