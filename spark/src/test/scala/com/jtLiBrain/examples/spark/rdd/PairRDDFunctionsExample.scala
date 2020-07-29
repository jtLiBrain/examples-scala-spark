package com.jtLiBrain.examples.spark.rdd

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

class PairRDDFunctionsExample extends FunSuite with SharedSparkContext {
  test("groupByKey") {
    val rdd = sc.parallelize(Seq(("a", 1)))
    rdd.groupByKey()
  }

  test("foldByKey") {
    val rdd = sc.parallelize(Seq(("a", 1)))
  }

  test("foldByKey") {
    val rdd = sc.parallelize(Seq(("a", 1)))
  }

  test("join between rdds") {
    val data1 = Seq((1, 1L), (1, 2L), (1, 3L), (2, 1L))
    val data2 = Seq((1, "11"), (1, "12"), (2, "22"), (3, "3"))

    val rdd1 = sc.parallelize(data1, 2)
    val rdd2 = sc.parallelize(data2, 2)

    val joinedRdd:RDD[(Int, (Long, String))] = rdd1.join(rdd2)
  }

  test("cogroup between rdds") {
    val data1 = Seq((1, 1L), (1, 2L), (1, 3L), (2, 1L))
    val data2 = Seq((1, "11"), (1, "12"), (2, "22"), (3, "3"))

    val rdd1 = sc.parallelize(data1, 2)
    val rdd2 = sc.parallelize(data2, 2)

    val joinedRdd:RDD[(Int, (Iterable[Long], Iterable[String]))] = rdd1.cogroup(rdd2)
  }
}