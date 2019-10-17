package com.jtLiBrain.examples.spark.elasticsearch

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RDDWithESExample{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("spark-elasticsearch")

    val esOptions = Map(
      "es.nodes" -> "192.168.56.101",
      "es.port" -> "9200"
    )

    val sparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    try {
      // TODO
    } finally {
      sparkSession.stop()
    }
  }


}
