package com.jtLiBrain.examples.spark.rdd

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite
import com.jtLiBrain.examples.spark.utils._
import org.apache.spark.Partitioner

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

  test("repartitionAndSortWithinPartitions") {
    val data = sc.parallelize(Seq(
      ((0, 5), 1),
      ((3, 8), 1),
      ((2, 6), 1),
      ((0, 8), 1),
      ((3, 8), 1),
      ((1, 3), 1)
    ), 2)

    val partitioner = new Partitioner {
      def numPartitions: Int = 2
      def getPartition(key: Any): Int = key.asInstanceOf[Tuple2[Int, Int]]._1 % 2
    }

    val repartitioned = data.repartitionAndSortWithinPartitions(partitioner)
    val partitions = repartitioned.glom().collect()
    partitions.foreach{arr =>
      println(arr.mkString(","))
    }
  }
}