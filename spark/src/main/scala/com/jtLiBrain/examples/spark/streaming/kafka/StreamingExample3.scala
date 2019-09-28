package com.jtLiBrain.examples.spark.streaming.kafka

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}


/*
{"id":1, "name":"user 1"}
{"id":2, "name":"user 2"}
{"id":3, "name":"user 3"}
{"id":4, "name":"user 4"}
{"id":5, "name":"user 5"}
{"id":6, "name":"user 6"}
{"id":7, "name":"user 7"}
{"id":8, "name":"user 8"}
{"id":9, "name":"user 9"}
{"id":10, "name":"user 10"}

 */
object StreamingExample3 {
  def formatTime(mills: Long): String = {
    val dateTimePattern = "yyyy-MM-dd HH:mm:ss"

    val sdf = new SimpleDateFormat(dateTimePattern)
    sdf.format(new Date(mills))
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf

    conf.setMaster("local[1]")
    conf.setAppName("Test")

    conf.set("spark.streaming.backpressure.enabled", "true")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "1")

    val streamingContext = new StreamingContext(conf, Seconds(3))

    try {
      val brokers = "localhost:9092"
      val groupId = "StreamingExample"
      val topic = "users"

      val kafkaParams = Map[String, Object](
        "bootstrap.servers"   -> brokers,
        "key.deserializer"    -> classOf[StringDeserializer],
        "value.deserializer"  -> classOf[StringDeserializer],
        "group.id"            -> groupId,
        /*"auto.offset.reset"   -> "latest",*/
        "auto.offset.reset"   -> "earliest",
        "enable.auto.commit"  -> (false: java.lang.Boolean)
      )


      val kafkaInputDStream = KafkaUtils.createDirectStream[String, String] (
        streamingContext,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams)
      )

      kafkaInputDStream.foreachRDD { (rdd, batchTime) =>
        val time = formatTime(batchTime.milliseconds)

        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        println()
        println(time + " --------------------- start")

        rdd.foreachPartition { iter =>
          val partitionId = TaskContext.get.partitionId

          println(s"partition:$partitionId data:")
          iter.foreach{ r =>
            println("\t" + r.value())
          }

          println(s"partition:$partitionId offset:")
          val o: OffsetRange = offsetRanges(partitionId)
          println(s"\ttopic:${o.topic} partition:${o.partition} fromOffset:${o.fromOffset} untilOffset:${o.untilOffset}")
        }

        println(time + " --------------------- end")
        println()


        kafkaInputDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }

      // start the streaming app
      streamingContext.start()
      streamingContext.awaitTermination()
    } finally {
      streamingContext.stop(true, true)
    }
  }
}
