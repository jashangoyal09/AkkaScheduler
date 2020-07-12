name := "AkkaScheduler"

version := "0.1"

scalaVersion := "2.13.3"

// https://mvnrepository.com/artifact/com.typesafe.akka/akka-actor
libraryDependencies ++= Seq("com.typesafe.akka" %% "akka-actor" % "2.6.7",
  "org.apache.kafka" % "kafka-clients" % "2.5.0",
  "org.json4s" %% "json4s-jackson" % "3.7.0-M4")



