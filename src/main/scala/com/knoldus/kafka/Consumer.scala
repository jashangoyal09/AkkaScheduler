package com.knoldus.kafka

import java.time.Duration
import java.util.{Collections, Properties}

import akka.actor.{Actor, ActorSystem, Props}
import com.knoldus.kafka.ConsumeRecords.ConsumeNews
import com.knoldus.models.{News, NewsDeserializer}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.jdk.CollectionConverters._

class ConsumeRecords extends Actor {
  override def receive: Receive = {
    case ConsumeNews =>
      consumer
  }
  def consumer: Unit = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaConsumer")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "groupId")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[NewsDeserializer])

    val consumer = new KafkaConsumer[String, News](props)
    consumer.subscribe(Collections.singletonList("topic"))
    val records = consumer.poll(Duration.ofMillis(1000)).iterator().asScala
    for (record <- records) {
      println("Receive message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
    }
    consumer.close()
  }
}

object ConsumeRecords {

  val system = ActorSystem("HelloSystem1")
  val cons = system.actorOf(Props[ConsumeRecords])
  case object ConsumeNews
}
