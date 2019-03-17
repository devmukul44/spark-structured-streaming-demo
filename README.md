# Spark Structured Streaming Demo App

## Structure of the code

### We can create the entry point of our application by writing a main function. In Scala, a static method needs to be in an object, not in a class, so let’s create one:

```scala
object ClickstreamExecutor {
  def main(args: Array[String]): Unit = {
    new StreamsProcessor("localhost:9092").process()
  }
}
```

### We can now initialize SparkSession and Logger:

```scala
   // SparkSession
    val spark = SparkSession.builder
      .master("yarn")
      .appName(getClass.getSimpleName)
      .enableHiveSupport()
      .getOrCreate()

    // Root Logger Level
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
```

### Reading Data Streams from Kafka

#### Code:

```scala
    val inputDf: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.140.10.108:9092, 10.140.10.103:9092, 10.140.10.11:9092")
      .option("subscribe", "kafka-clickstream-topic")
      .load()
```

#### Input Streaming DataFrame  

**Properties:**  
* **readStream**                ->  read streaming data in as a `DataFrame`
* **format**                    ->  Specifies the input data source format. - `kafka`, `file_system`...
* **kafka.bootstrap.servers**   ->  Stringified List of Kafka host ips
* **subscribe**                 ->  Kafka Topic Name

**Schema:**
    
```$xslt
 root
      |-- key: binary (nullable = true)
      |-- value: binary (nullable = true)
      |-- topic: string (nullable = true)
      |-- partition: integer (nullable = true)
      |-- offset: long (nullable = true)
      |-- timestamp: timestamp (nullable = true)
      |-- timestampType: integer (nullable = true)
```

### Preparing the output

#### Output Sink
**Output Sink Properties:**
* **writeStream**         ->  saving the content of the streaming Dataset out into external storage.
* **partitionBy**         ->  output partition columns
* **outputMode**          ->  Type of output sink - `Append`, `complete`, `update`
* **trigger**             ->  Trigger for spark mini batch stream query
* **format**              ->  Output format - `kafka`, `parquet` ...
* **path**                ->  Output Location for the `file_system` ...
* **checkpointLocation**  ->  Location for saving streaming metadata - `write-ahead-logs`, `kafka-offsets`, `aggregated-data` ...


**streamingQuery Properties:**
* **awaitTermination**    ->  Waits for the termination of `this` query, either by `query.stop()` or by an exception.
* **stop**                ->  Stops the execution of this query if it is running.
* **status**              ->  Returns the current status of the query.

#### Code:
```scala
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

```