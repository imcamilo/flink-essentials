package io

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

import java.io.{FileWriter, PrintWriter}
import java.net.{ServerSocket, Socket}
import java.util.Scanner

object CustomSinks {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  private val stringStream: DataStream[String] = env.fromElements(
    "Lorem Ipsum is simply dummy text of the printing and typesetting industry.",
    "Lorem Ipsum has been the industry's standard dummy text ever since the 1500s,",
    "when an unknown printer took a galley of type and scrambled it to make a type specimen book.",
    "It has survived not only five centuries,",
    "but also the leap into electronic typesetting,",
    "remaining essentially unchanged.",
    "It was popularised in the 1960s with the release of Letraset sheets containing",
    "Lorem Ipsum passages, and more recently with desktop publishing software",
    "like Aldus PageMaker including versions of Lorem Ipsum.",
  )

  //push the strings to a file sink. Instantiate once per thread
  private class FileSink(path: String) extends SinkFunction[String] {
    //up to us overwrite, context provide us timestamps or watermarks if necessary
    override def invoke(event: String, context: SinkFunction.Context): Unit = {
      //open a file in that path
      val writer = new PrintWriter(new FileWriter(path, true)) //append mode //not optimal
      writer.println(event)
      writer.flush()
      writer.close()
    }
  }

  private class RichFileSink(path: String) extends RichSinkFunction[String] {
    //hold state and lifecycle methods

    private var writer: PrintWriter = _

    override def open(parameters: Configuration): Unit = {
      writer = new PrintWriter(new FileWriter(path, true)) //append mode //not optimal
    }

    //up to us overwrite, context provide us timestamps or watermarks if necessary
    override def invoke(event: String, context: SinkFunction.Context): Unit = {
      //open a file in that path
      writer.println(event)
      writer.flush()
    }

    override def close(): Unit = {
      writer.close()
    }

  }

  private def demoFileSink(): Unit = {
    //stringStream.addSink(new FileSink("output/demoFileSink.txt"))
    stringStream.addSink(new RichFileSink("output/demoFileSink.txt"))
    stringStream.print()
    env.execute()
  }

  /*
    create a sink function that will push data to a socket sink.
   */
  private class SocketSink(host: String, port: Int) extends RichSinkFunction[String] {

    var socket: Socket = _
    var writer: PrintWriter = _

    override def open(parameters: Configuration): Unit = {
      socket = new Socket(host, port)
      writer = new PrintWriter(socket.getOutputStream)
    }

    override def invoke(value: String, context: SinkFunction.Context): Unit = {
      writer.println(value)
      writer.flush()
    }

    override def close(): Unit = {
      socket.close()
    }
  }

  def demoSocketSink(): Unit = {
    stringStream.addSink(new SocketSink("localhost", 12345)).setParallelism(1)
    stringStream.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    //demoFileSink()
    demoSocketSink()
  }

}

/**
 * - start data receiver
 * - start flink
 */
object DataReceiver {
  def main(args: Array[String]): Unit = {
    val server = new ServerSocket(12345)
    println("Waiting for Flink to connect...")
    val socket = server.accept()
    val reader = new Scanner(socket.getInputStream)
    println("Flink connected. Reading...")
    while (reader.hasNextLine) {
      println(s"${reader.nextLine()}")
    }
    socket.close()
    println("All data read. Closing app.")
    server.close()
  }
}
