package com.jtLiBrain.examples.spark.rdd

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner}
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class OrderedRDDFunctionsExample extends FunSuite with SharedSparkContext {
  /**
   * 1. 先按key进行分区
   * 2. 分区内部根据key对数据进行排序
   */
  test("repartitionAndSortWithinPartitions") {
    val rdd = sc.parallelize(Seq(
      ("a", 2), ("a", 3), ("a", 1),
      ("b", 3), ("b", 2),
      ("c", 2), ("c", 3), ("c", 1)
    ))

    val sortedRDD = rdd.repartitionAndSortWithinPartitions(new HashPartitioner(2))
    var partIdx = 0
    sortedRDD.glom().collect().foreach { case dataItem =>
      dataItem.foreach { case (k, v) =>
        println(s"parttioin:$partIdx, key:$k, value:$v")
      }
      partIdx += 1
    }
    /*
    parttioin:0, key:b, value:3
    parttioin:0, key:b, value:2
    parttioin:1, key:a, value:2
    parttioin:1, key:a, value:3
    parttioin:1, key:a, value:1
    parttioin:1, key:c, value:2
    parttioin:1, key:c, value:3
    parttioin:1, key:c, value:1

     */
  }

  test("repartitionAndSortWithinPartitions - custom ordering") {
    val rdd = sc.parallelize(Seq(
      ("a", 2), ("a", 3), ("a", 1),
      ("b", 3), ("b", 2),
      ("c", 2), ("c", 3), ("c", 1)
    ))

    implicit val caseInsensitiveOrdering = new Ordering[String] {
      override def compare(a: String, b: String) = a.compare(b) * -1
    }
    val sortedRDD = rdd.repartitionAndSortWithinPartitions(new HashPartitioner(2))
    var partIdx = 0
    sortedRDD.glom().collect().foreach { case dataItem =>
      dataItem.foreach { case (k, v) =>
        println(s"parttioin:$partIdx, key:$k, value:$v")
      }
      partIdx += 1
    }
    /*
    parttioin:0, key:b, value:3
    parttioin:0, key:b, value:2
    parttioin:1, key:c, value:2
    parttioin:1, key:c, value:3
    parttioin:1, key:c, value:1
    parttioin:1, key:a, value:2
    parttioin:1, key:a, value:3
    parttioin:1, key:a, value:1
     */
  }

  test("repartitionAndSortWithinPartitions - totally custom") {
    class PrimaryKeyPartitioner[K, S](partitions: Int) extends Partitioner {
      val delegatePartitioner = new HashPartitioner(partitions)
      override def numPartitions: Int = delegatePartitioner.numPartitions

      // 根据"键"中的第一个部分进行分区
      override def getPartition(key: Any): Int = {
        val k = key.asInstanceOf[(K, S)]
        delegatePartitioner.getPartition(k._1)
      }
    }

    def groupByKeyAndSortBySecondaryKey[K : Ordering : ClassTag,
      S : Ordering : ClassTag,
      V : ClassTag]
    (pairRDD : RDD[((K, S), V)], partitions : Int):
    RDD[(K, List[(S, V)])] = {
      //Create an instance of our custom partitioner
      val colValuePartitioner = new PrimaryKeyPartitioner[Double, Int](partitions)
      //define an implicit ordering, to order by the second key the ordering will
      //be used even though not explicitly called
      implicit val ordering: Ordering[(K, S)] = Ordering.Tuple2
      //use repartitionAndSortWithinPartitions
      val sortedWithinParts =
        pairRDD.repartitionAndSortWithinPartitions(colValuePartitioner)
      sortedWithinParts.mapPartitions( iter => groupSorted[K, S, V](iter) )
    }

    def groupSorted[K,S,V](it: Iterator[((K, S), V)]): Iterator[(K, List[(S, V)])] = {
      val res = List[(K, ArrayBuffer[(S, V)])]()
      it.foldLeft(res)((list, next) => list match {
        case Nil =>
          val ((firstKey, secondKey), value) = next
          List((firstKey, ArrayBuffer((secondKey, value))))
        case head :: rest =>
          val (curKey, valueBuf) = head
          val ((firstKey, secondKey), value) = next
          if (!firstKey.equals(curKey) ) {
            (firstKey, ArrayBuffer((secondKey, value))) :: list
          } else {
            valueBuf.append((secondKey, value))
            list
          }
      }).map { case (key, buf) => (key, buf.toList) }.iterator
    }
  }
}
