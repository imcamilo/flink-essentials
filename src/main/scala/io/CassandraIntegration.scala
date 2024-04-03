package io

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.cassandra.CassandraSink

object CassandraIntegration {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  case class Person(name: String, age: Int)

  // write data to cassandra
  def demoWriteDataToCassandra(): Unit = {
    val people = env.fromElements(
      Person("Daniel", 32),
      Person("Alice", 12),
      Person("Julie", 14),
      Person("Mal", 31),
    )

    // we can only writes TUPLES to cassandra
    val personTuples: DataStream[(String, Int)] = people.map(p => (p.name, p.age))

    //writting the data
    CassandraSink //builder pattern
      .addSink(personTuples)
      .setQuery("insert into rtjvm.people(name, age) values (?, ?)")
      .setHost("localhost")
      .build()

    env.execute()

  }

  def main(args: Array[String]): Unit = {
    demoWriteDataToCassandra()
  }


}
