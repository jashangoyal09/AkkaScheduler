package com.knoldus.models

import java.sql.Timestamp
import java.util.Properties

import com.knoldus.kafka.{ObjectDeserializer, ObjectSerializer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random

case class News(id: String, title: String, date: Timestamp, priority: Int) {

  def produce: Unit = {
    val source = List("BBC", "The Hindu", "Knoldus")
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("client.id", "KafkaProducer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ObjectSerializer[News]])
    val producer = new KafkaProducer[String, News](props)
    val random = new Random()
    val data = new ProducerRecord[String, News]("topic", source(random.nextInt(3)), this)
    producer.send(data)
    println("Produce message: ( " + this + " )")
    producer.close()
  }

}

class NewsDeserializer extends ObjectDeserializer[News](classOf[News])
