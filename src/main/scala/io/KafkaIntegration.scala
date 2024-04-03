package io

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema, SimpleStringSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object KafkaIntegration {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  // read simple data (strings) from kafka topic
  private def readStrings(): Unit = {
    val kafkaSource = KafkaSource.builder[String]()
      .setBootstrapServers("localhost:9092")
      .setTopics("events")
      .setGroupId("events-group")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val kafkaStrings: DataStream[String] =
      env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")

    // use DS
    kafkaStrings.print()
    env.execute()

  }

  //read custom data
  case class Person(name: String, age: Int)

  // need the serializer
  private class PersonDeserializer extends DeserializationSchema[Person] {

    override def deserialize(message: Array[Byte]): Person = {
      // format: name, age, maybe json, maybe xml, etc
      val string = new String(message)
      val tokens = string.split(",")
      val name = tokens(0).trim
      val age = tokens(1).trim
      Person(name, age.toInt)
    }

    override def isEndOfStream(nextElement: Person): Boolean = false

    override def getProducedType: TypeInformation[Person] = implicitly[TypeInformation[Person]]

  }

  private def readCustomData(): Unit = {
    val kafkaSource = KafkaSource.builder[Person]()
      .setBootstrapServers("localhost:9092")
      .setTopics("people")
      .setGroupId("people-group")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new PersonDeserializer())
      .build()

    val kafkaPeopleStream: DataStream[Person] =
      env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")

    kafkaPeopleStream.print()
    env.execute()
  }

  // write custom data
  private class PersonSerializer extends SerializationSchema[Person] {
    override def serialize(person: Person): Array[Byte] = {
      val utf = "UTF-8"
      s"${person.name}, ${person.age}".getBytes(utf)
    }
  }

  private def writeCustomData(): Unit = {
    val kafkaSink = new FlinkKafkaProducer[Person](
      "localhost:9092", // boostrap server
      "people", // topic
      new PersonSerializer // serializer
    )

    val peopleStream = env.fromElements(
      Person("Camilo", 29),
      Person("Yen", 25),
      Person("Gus", 30),
      Person("Alice", 29),
      Person("Charli", 43),
    )

    peopleStream.addSink(kafkaSink)
    peopleStream.print()
    env.execute()

  }

  def main(args: Array[String]): Unit = {
    //readStrings()
    //readCustomData()
    writeCustomData()
  }

}
