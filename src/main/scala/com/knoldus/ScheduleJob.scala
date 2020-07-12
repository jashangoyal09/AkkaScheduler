package com.knoldus

import java.sql.Timestamp
import java.util.UUID

import akka.actor._
import com.knoldus.ScheduleJob.{ProduceRecords, ScheduleMessageProducer}
import com.knoldus.kafka.ConsumeRecords
import com.knoldus.kafka.ConsumeRecords.ConsumeNews
import com.knoldus.models.News

import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.Random

class ScheduleJob extends Actor {

  override def preStart(): Unit = {
    self ! ScheduleMessageProducer(0.seconds, 5.seconds)
  }

  override def receive: Receive = {
    case ScheduleMessageProducer(initialDelay, interval) =>
      scheduler.scheduleAtFixedRate(initialDelay,
        interval,
        self,
        ProduceRecords
      )(context.dispatcher)
    case ProduceRecords =>
      val random = new Random()
      val news = News(UUID.randomUUID().toString, "titleName " + ('a' + random.nextPrintableChar), new Timestamp(System.currentTimeMillis()), random.nextInt(10))
      news.produce
      ConsumeRecords.cons ! ConsumeNews
  }

  def scheduler: Scheduler = context.system.scheduler
}

object ScheduleJob extends App {

  val system = ActorSystem("Scheduler")
  system.actorOf(Props[ScheduleJob])

  final case class ScheduleMessageProducer(initialDelay: FiniteDuration, interval: FiniteDuration)

  case object ProduceRecords

}
