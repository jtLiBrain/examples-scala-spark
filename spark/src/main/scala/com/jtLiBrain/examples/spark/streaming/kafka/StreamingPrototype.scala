package com.jtLiBrain.examples.spark.streaming.kafka

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.jtLiBrain.examples.spark.SparkSubmitParams
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Durations, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingPrototype extends Logging{
  def main(args: Array[String]): Unit = {
    val conf = defineConfs()
    val sc = new SparkContext(conf)

    sc.setLogLevel(conf.get(SparkSubmitParams.LOG_LEVEL, "WARN"))

    try {
      while (true) {
        try {
          launchStreaming(sc, conf)
        } catch {
          // FIXME restarting new streaming should only for some Exception e.g. some exception from Kafka
          case e: Throwable =>
            logError("Try to re-launch a new streaming task", e)
            TimeUnit.SECONDS.sleep(1)
        }
      }
    } finally {
      sc.stop()
    }
  }

  def defineConfs(): SparkConf = {
    val conf = new SparkConf()

    conf.setMaster("local[*]")
    conf.setAppName("Spark Streaming App")

    conf.set(SparkSubmitParams.STREAMING_BATCH_INTERVAL,          "30s")
    conf.set(SparkSubmitParams.KAFKA_OPTIONS_BOOTSTRAP_SERVERS,   "192.168.56.101:9092")
    conf.set(SparkSubmitParams.KAFKA_TOPIC,                       "test")
    conf.set(SparkSubmitParams.KAFKA_OPTIONS_GROUP_ID,            "spark_streaming_test")
    conf.set(SparkSubmitParams.KAFKA_OPTIONS_AUTO_OFFSET_RESET,   "latest")
    conf.set(SparkSubmitParams.KAFKA_OPTIONS_ENABLE_AUTO_COMMIT,  "false")

    conf
  }

  def launchStreaming(sc: SparkContext, conf: SparkConf): Unit = {
    val batchInterval = conf.get(SparkSubmitParams.STREAMING_BATCH_INTERVAL)
    val batchInMS = JavaUtils.timeStringAs(batchInterval, TimeUnit.MILLISECONDS)

    val sparkSession = SparkSession.builder.config(conf).getOrCreate()
    val streamingContext = new StreamingContext(sc, Durations.milliseconds(batchInMS))

    try {
      val kafkaInputDStream = defineSource(streamingContext, conf)

      defineLogic(sparkSession, conf)(kafkaInputDStream)

      streamingContext.start()
      streamingContext.awaitTermination()
    } finally {
      streamingContext.stop(false, true)
    }
  }

  def defineSource(streamingContext: StreamingContext, conf: SparkConf): InputDStream[ConsumerRecord[String, String]] = {
    val kafkaTopic = conf.get(SparkSubmitParams.KAFKA_TOPIC)
    val kafkaGroupDynamic = conf.getBoolean(SparkSubmitParams.KAFKA_GROUP_DYNAMIC, false)
    val kafkaGroupId = conf.get(SparkSubmitParams.KAFKA_OPTIONS_GROUP_ID)

    val kafkaParams = collection.mutable.Map[String, Object] (
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val kafkaConfs = conf.getAllWithPrefix(SparkSubmitParams.KAFKA_OPTIONS_PREFIX)
    if(kafkaConfs != null && !kafkaConfs.isEmpty) {
      kafkaConfs.foreach{case (k, v) => kafkaParams += k -> v }
    }

    if(kafkaGroupDynamic) {
      kafkaParams += (ConsumerConfig.GROUP_ID_CONFIG -> (kafkaGroupId+System.currentTimeMillis()))
    }

    logWarning(s"Kafka params are:\n${kafkaParams.mkString("\n")}")

    // create DStream from Kafka
    KafkaUtils.createDirectStream[String, String] (
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafkaTopic), kafkaParams.toMap/*, offset*/)
    )
  }

  def defineLogic(sparkSession: SparkSession, conf: SparkConf)
                 (kafkaInputDStream: InputDStream[ConsumerRecord[String, String]]): Unit = {

    kafkaInputDStream.foreachRDD{ (kafkaRdd, batchTime) =>
      val offsetRanges = kafkaRdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // compute logic
      computeLogic(sparkSession)(kafkaRdd, batchTime)

      // commit offsets
      commitOffsets(kafkaInputDStream, offsetRanges)
    }
  }

  def commitOffsets(kafkaInputDStream: InputDStream[ConsumerRecord[String, String]], offsetRanges: Array[OffsetRange]): Unit = {

  }

  def computeLogic(sparkSession: SparkSession)
                     (rdd: RDD[ConsumerRecord[String, String]], batchTime:Time): Unit = {

    val timestamp = new Timestamp(batchTime.milliseconds)

    println(s"Batch[${timestamp.toString}] >>>>>>>>>>>>>>>>>")

    rdd.map(_.value()).collect().foreach{kafkaRV =>
      println("\t" + kafkaRV)
    }
    println(s"Batch[${timestamp.toString}] <<<<<<<<<<<<<<<<<")

  }
}
