package com.whiletruecurious.sparkstreaming.executor

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{DataType, StringType, StructType}

import scala.collection.JavaConverters._

/** Clickstream Spark Structured Streaming Executor
  *
  * Developed By Mukul Dev on 16th March, 2019
  */
object ClickstreamExecutor {
  def main(args: Array[String]): Unit = {

    // SparkSession
    val spark = SparkSession.builder
      .master("yarn")
      .appName(getClass.getSimpleName)
      .enableHiveSupport()
      .getOrCreate()

    // Root Logger Level
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    // Loading Configuration File
    val config = ConfigFactory.load()
    // Input Config
    val inputConfig = config.getConfig("input")
    val inputSourceType = inputConfig.getString("type")
    assert(inputSourceType.equalsIgnoreCase("kafka"), "Only is supported as Input Type")
    val inputKafkaServer = inputConfig.getString("server")
    val inputKafkaTopic = inputConfig.getString("topic")
    // Output Config
    val outputConfig = config.getConfig("output")
    val outputPartitionColumnList = outputConfig.getStringList("partition_columns").asScala.toList
    val outputModeType = OutputMode.Append()
    val outputMiniBatchTriggerInterval = outputConfig.getString("trigger_interval")
    val outputFormat = outputConfig.getString("type")
    val outputLocation = outputConfig.getString("output_location")
    val outputCheckpointLocation = outputConfig.getString("checkpoint_location")

    /** Input Streaming DataFrame
      *
      * Properties:
      * readStream                ->  read streaming data in as a `DataFrame`
      * format                    ->  Specifies the input data source format. - `kafka`, `file_system`...
      * kafka.bootstrap.servers   ->  Stringified List of Kafka host ips
      * subscribe                 ->  Kafka Topic Name
      *
      * Schema:
      * root
      * |-- key: binary (nullable = true)
      * |-- value: binary (nullable = true)
      * |-- topic: string (nullable = true)
      * |-- partition: integer (nullable = true)
      * |-- offset: long (nullable = true)
      * |-- timestamp: timestamp (nullable = true)
      * |-- timestampType: integer (nullable = true)
      */
    val inputDf: DataFrame = spark
      .readStream
      .format(inputSourceType)
      .option("kafka.bootstrap.servers", inputKafkaServer)
      .option("subscribe", inputKafkaTopic)
      .load()

    // Casting Binary `value` to String
    val valueJsonStringDF: DataFrame = inputDf.select(col("value").cast(StringType))

    // Schema of Input Kafka Messages
    val schemaStruct: StructType = new StructType().add("name", StringType).add("birthDate", StringType)

    /** Providing Schema to JSON string
      * At this point, the DataFrame contains nested columns, as indicated by the schema:
      *
      * Schema:
      * root
      * |-- person: struct (nullable = true)
      * |    |-- name: string (nullable = true)
      * |    |-- birthDate: string (nullable = true)
      *
      */
    val valueJsonDF: DataFrame = valueJsonStringDF.select(from_json(col("value"), schemaStruct).as("person"))

    /** Flat Input Stream DataFrame
      *
      * Schema:
      *
      * root
      * |-- name: string (nullable = true)
      * |-- birthDate: string (nullable = true)
      */
    val flattenedValueDF = valueJsonDF.select("person.*")

    /** Output Sink
      *
      * Output Sink Properties:
      * writeStream         ->  saving the content of the streaming Dataset out into external storage.
      * partitionBy         ->  output partition columns
      * outputMode          ->  Type of output sink - `Append`, `complete`, `update`
      * trigger             ->  Trigger for spark mini batch stream query
      * format              ->  Output format - `kafka`, `parquet` ...
      * path                ->  Output Location for the `file_system` ...
      * checkpointLocation  ->  Location for saving streaming metadata - `write-ahead-logs`, `kafka-offsets`, `aggregated-data` ...
      *
      * streamingQuery Properties:
      * awaitTermination    ->  Waits for the termination of `this` query, either by `query.stop()` or by an exception.
      * stop                ->  Stops the execution of this query if it is running.
      * status              ->  Returns the current status of the query.
      */

    // File Output Sink
    val fileOutputSink = flattenedValueDF
      .writeStream
      .partitionBy(outputPartitionColumnList: _*)
      .outputMode(outputModeType)
      .trigger(Trigger.ProcessingTime(outputMiniBatchTriggerInterval))
      .format(outputFormat)
      .option("path", outputLocation)
      .option("checkpointLocation", outputCheckpointLocation)
      .start()

    fileOutputSink.awaitTermination()
    fileOutputSink.stop()
    fileOutputSink.status

    // Kafka Output Sink
    val kafkaOutputSink = flattenedValueDF
      .writeStream
      .outputMode(outputModeType)
      .trigger(Trigger.ProcessingTime(outputMiniBatchTriggerInterval))
      .format("kafka")
      .option("kafka.bootstrap.servers", inputKafkaServer)
      .option("subscribe", inputKafkaTopic)
      .option("checkpointLocation", outputCheckpointLocation)
      .start()

    // Console Output Sink
    val consoleOutputSink = flattenedValueDF
      .writeStream
      .outputMode(outputModeType)
      .trigger(Trigger.ProcessingTime(outputMiniBatchTriggerInterval))
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", outputCheckpointLocation)
      .start()
  }
}
