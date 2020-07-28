package com.jtLiBrain.examples.spark.rdd

import com.holdenkarau.spark.testing.SharedSparkContext
import com.jtLiBrain.examples.spark.utils._
import org.apache.spark.Partitioner
import org.scalatest.FunSuite

class RDDFunctionsExample extends FunSuite with SharedSparkContext {
  /**
   * zipWithIndex的原理是：
   * 1. 为每个分区计算数据项的起始索引，startIndex；
   * 2. 当前分区内，每个项目数据的索引为：startIndex + n，n为当前数据项在该分区的索引位置，zipWithIndex并没有在分区内对数据进行排序操作
   * 如果想要得到数据排序后的索引，那么可以使用类似sortBy、sortByKey这样的转换操作，得到一个排序RDD，其分区内的数据是依数据排序的
   */
  test("zipWithIndex") {
    val rdd = sc.parallelize(Seq(9, 7, 4, 5, 2, 4, 1), 3)
    val zipRdd = rdd.zipWithIndex()
    zipRdd.collect().foreach { case (value, index) =>
      println(s"value:$value, index:$index")
    }
    println("-----------------------")
    val zipRdd2 = rdd.sortBy(n => n).zipWithIndex()
    zipRdd2.collect().foreach { case (value, index) =>
      println(s"value:$value, index:$index")
    }
  }

}