package io

import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._

import java.sql.PreparedStatement

object JDBCIntegration {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  //write data to jdbc, no matters how data flow to flink
  case class Person(name: String, age: Int)

  private def demoWriteToJDBC(): Unit = {
    val people = env.fromElements(
      Person("Dan", 33),
      Person("Alice", 3),
      Person("Bob", 23),
      Person("Mary", 43),
    )
    //jdbc sink
    val jdbcSink: SinkFunction[Person] = JdbcSink.sink[Person](
      //1. SQL statement
      "insert into people(name, age) values(?, ?)",
      //2. statement builder
      new JdbcStatementBuilder[Person] { // the way to expand wildcards with actual values
        override def accept(statement: PreparedStatement, person: Person): Unit = {
          statement.setString(1, person.name) // the first ? is replaced with person.name
          statement.setInt(2, person.age) // similar
        }
      },
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:postgresql://localhost:5432/rtjvm")
        .withDriverName("org.postgresql.Driver")
        .withUsername("docker")
        .withPassword("docker")
        .build()
    )

    //push the data through the sink
    people.addSink(jdbcSink)
    people.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    demoWriteToJDBC()
  }

}
