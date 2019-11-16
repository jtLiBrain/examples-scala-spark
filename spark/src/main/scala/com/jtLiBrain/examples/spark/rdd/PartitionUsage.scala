package com.jtLiBrain.examples.spark.rdd

import org.apache.spark.sql.SparkSession

object PartitionUsage {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession
        .builder
        .master("local[*]")
        .appName("Partition usage")
        .getOrCreate()

    val sc = ss.sparkContext

    try {

    } finally {
      ss.stop()
    }
  }
}
