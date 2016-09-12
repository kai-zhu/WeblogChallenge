import Build._

name := "WebLogChallenge"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion withSources() withJavadoc() exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark" %% "spark-sql" % sparkVersion withSources() withJavadoc(),
  "joda-time" % "joda-time" % jodaTimeVersion withSources() withJavadoc(),
  "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion  withSources() withJavadoc(),
  "ch.qos.logback" % "logback-classic" % logbackVersion withSources() withJavadoc(),
  "org.specs2" %% "specs2" % specs2Version,
  "org.scalatest" %% "scalatest" % scalatestVersion % Test withSources() withJavadoc(),
  "org.testng" % "testng" % testngVersion % Test withSources() withJavadoc()
)