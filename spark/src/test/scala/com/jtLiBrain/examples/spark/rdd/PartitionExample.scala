package com.jtLiBrain.examples.spark.rdd

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

import com.jtLiBrain.examples.spark.utils._

class PartitionExample extends FunSuite with SharedSparkContext {
  test("compute") {
    val rdd = sc.parallelize(1 to 10, 3)

    // method 1
    sc.collectAsPartitions(rdd).foreach {r =>
      println(s"Partition: ${r._1}\n    ${r._2.mkString(", ")}")
    }

    // method 2
    rdd.printAsPartitions()
  }
}