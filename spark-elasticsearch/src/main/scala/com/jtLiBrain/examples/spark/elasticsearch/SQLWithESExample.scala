package com.jtLiBrain.examples.spark.elasticsearch

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.elasticsearch.spark.sql._

/**
  * org.apache.spark.sql.execution.datasources.csv.CSVOptions.scala
  */
object SQLWithESExample{

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
    import sparkSession.implicits._

    try {
      val rawDF = sparkSession.read.option("header", "true")
        .option("sep", ";")
        .csv("data/people.csv")

      rawDF.show()
      rawDF.saveToEs("spark/people", esOptions)
    } finally {
      sparkSession.stop()
    }
  }
}
