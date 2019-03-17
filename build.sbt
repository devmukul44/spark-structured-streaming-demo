name := "spark-structured-streaming"
organization := "com.whiletruecurious"
version := "0.1"

scalaVersion := "2.11.8"


val sparkVersion = "2.3.0"

// Spark Dependencies
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion

// Scala Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"

// Additional Dependencies
// Typesafe Config
libraryDependencies += "com.typesafe" % "config" % "1.3.2"
// Joda Time
libraryDependencies += "joda-time" % "joda-time" % "2.9.9"
// Elasticsearch
libraryDependencies += "org.elasticsearch" % "elasticsearch-hadoop" % "6.3.1"
// Cron Scheduler
libraryDependencies += "org.quartz-scheduler" % "quartz" % "2.2.1"
// Avro Connector
libraryDependencies += "com.databricks" %% "spark-avro" % "4.0.0"
// AWS S3
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.438"
// User Agent Parser
libraryDependencies += "eu.bitwalker" % "UserAgentUtils" % "1.21"

// Merge Strategy
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
