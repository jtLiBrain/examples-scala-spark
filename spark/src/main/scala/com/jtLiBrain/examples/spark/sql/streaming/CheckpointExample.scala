package com.jtLiBrain.examples.spark.sql.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, OutputMode, Trigger}
import org.apache.spark.sql.functions._

object CheckpointExample {
  def main1(args: Array[String]): Unit = {
    val conf = new SparkConf(true)

    val sparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    import sparkSession.implicits._

    val kafkaStreamDF = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.16.182.246:9092")
      //.option("startingOffsets", "{\"test\":{\"1\":6,\"0\":6}}")
      .option("startingOffsets", "earliest")
      //.option("startingOffsets", "latest")
      .option("maxOffsetsPerTrigger", "1")
      .option("subscribe", "test")
      .load()

    val resultDF = kafkaStreamDF.selectExpr("CAST(value AS STRING)")
      .select(
        get_json_object($"value", "$.id").alias("id"),
        get_json_object($"value", "$.name").alias("name"),
        current_timestamp()/*.alias("aTime")*/
      )

    val query = resultDF.writeStream
      .outputMode(OutputMode.Append())
      .foreach(new MyForeachWriter)
      .trigger(Trigger.ProcessingTime("1 seconds"))
      //      .option("checkpointLocation", "file:///E:\\work\\codebase\\his-big-data\\data-analysis\\examples\\checkpoint")
      .start()



    query.awaitTermination()

    sparkSession.stop()
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true)

    conf.set("spark.sql.shuffle.partitions", "2")

    val sparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    try {
      val sourceDF = configureDataStreamReader(sparkSession).load()
      val finalDF  = transferDataFrame(sparkSession, sourceDF)
      val query    = configureDataStreamWriter(sparkSession, finalDF).start()
      query.awaitTermination()
    } finally {
      sparkSession.stop()
    }
  }

  def configureDataStreamReader(sparkSession: SparkSession): DataStreamReader = {
    sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.16.182.246:9092")
      //.option("startingOffsets", "{\"test\":{\"1\":6,\"0\":6}}")
      .option("startingOffsets", "earliest")
      //.option("startingOffsets", "latest")
      .option("maxOffsetsPerTrigger", "5")
      .option("subscribe", "test")
  }

  def configureDataStreamWriter(sparkSession: SparkSession, df: DataFrame): DataStreamWriter[Row] = {
    df.writeStream
      .outputMode(OutputMode.Append())
      .foreach(new MyForeachWriter)
      .trigger(Trigger.ProcessingTime("1 seconds"))
    //      .option("checkpointLocation", "file:///E:\\work\\codebase\\his-big-data\\data-analysis\\examples\\checkpoint")
  }

  def transferDataFrame(sparkSession: SparkSession, df: DataFrame): DataFrame = {
    import sparkSession.implicits._

    df.selectExpr("CAST(value AS STRING)")
      .select(
        get_json_object($"value", "$.id").alias("id"),
        current_timestamp().alias("aTime")
      ).withWatermark("aTime", "1 seconds")
      .groupBy(
        window($"aTime", "1 second")
      )
      .agg(
        approx_count_distinct("id").alias("countId"),
        collect_list($"id")
      )
  }
}

class MyForeachWriter extends ForeachWriter[Row] {
  override def open(partitionId: Long, version: Long): Boolean = {
    true
  }
  override def close(errorOrNull: Throwable): Unit = {
  }
  override def process(value: Row): Unit = {
    /*val ts = value.getTimestamp(2)
    ts.getDate
    println(ts.getDate)
    println(value.get(3).getClass)*/

    println(value.mkString("\t"))
  }
}