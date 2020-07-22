package com.jtLiBrain.examples.spark.rdd

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.{HashPartitioner}
import org.apache.spark.rdd.{CoGroupedRDD, RDD}
import org.scalatest.FunSuite

class CoGroupedRDDExample extends FunSuite with SharedSparkContext {
  /**
   * CoGroupedRDD的作用是将上游RDDs进行cogroups，
   * 返回的结果RDD中，对于父RDDs中的每个key，都有一个元组：
   * 元素第一项为key；
   * 第二项为Array[Iterable[_]]，其中数据组索引位置的值对应父RDD中对应该key的values集合
   */
  test("CoGroupedRDD") {
    val data1 = Seq((1, 1), (1, 2), (1, 3), (2, 1))
    val data2 = Seq((1, "11"), (1, "12"), (2, "22"), (3, "3"))

    val rdd1 = sc.parallelize(data1, 2)
    val rdd2 = sc.parallelize(data2, 2)

    val coGroupedRDD = new CoGroupedRDD[Int](
      Seq(rdd1, rdd2),
      new HashPartitioner(2)
    )

    val results = coGroupedRDD.map(p => (p._1, p._2.map(_.toArray))).collectAsMap()
    results.foreach{case (k,v) =>
      println(s"key:$k, value:")
      v.foreach(tv => println(s"\t${tv.mkString("[", ", ", "]")}"))
    }
  }
}