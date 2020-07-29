package com.jtLiBrain.examples.spark.rdd

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.HashPartitioner
import org.scalatest.FunSuite

class HowToHoldPartitionerExample extends FunSuite with SharedSparkContext {
  test("partitionBy") {
    val rdd = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 1), ("c", 1)), 2)

    val partitionByRdd = rdd.partitionBy(new HashPartitioner(2))
    assert(partitionByRdd.partitioner.isDefined)
  }

  test("repartition does't hold partitioning for downstream RDD") {
    val rdd = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 1), ("c", 1)), 2)

    val partitionByRdd = rdd.repartition(3)
    assert(partitionByRdd.partitioner.isEmpty)
  }

  test("mapValues") {
    val rdd = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 1), ("c", 1)), 2)

    val sortedRdd = rdd.sortByKey()
    assert(sortedRdd.partitioner.isDefined)

    // case 1
    val mapValuesFromSortedRdd = sortedRdd.mapValues(_+1)
    assert(mapValuesFromSortedRdd.partitioner.isDefined)

    // case 2
    val mapFromSortedRdd = sortedRdd.map{case (k,v) =>
      (k, v+1)
    }
    assert(mapFromSortedRdd.partitioner.isEmpty)
  }

  test("flatMapValues") {
    val rdd = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 1), ("c", 1)), 2)

    val sortedRdd = rdd.sortByKey()
    assert(sortedRdd.partitioner.isDefined)

    // case 1
    val flatMapValuesFromSortedRdd = sortedRdd.flatMapValues{ n =>
      Iterator(n)
    }
    assert(flatMapValuesFromSortedRdd.partitioner.isDefined)
  }

  test("mapPartitions") {
    val rdd = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 1), ("c", 1)), 2)

    val sortedRdd = rdd.sortByKey()
    assert(sortedRdd.partitioner.isDefined)

    // case 1
    val mapPartitionsFromSortedRdd1 = sortedRdd.mapPartitions((iter =>
      iter.map{case (k, v) => (k, v+1)}
    ), true)
    assert(mapPartitionsFromSortedRdd1.partitioner.isDefined)

    // case 2
    val mapPartitionsFromSortedRdd2 = sortedRdd.mapPartitions((iter =>
      iter.map{case (k, v) => (k, v+1)}
      ), false)
    assert(mapPartitionsFromSortedRdd2.partitioner.isEmpty)
  }

  test("sortByKey") {
    val rdd = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 1), ("c", 1)), 2)

    val sortedRdd = rdd.sortByKey()
    assert(sortedRdd.partitioner.isDefined)
  }

  test("repartitionAndSortWithinPartitions") {
    val rdd = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 1), ("c", 1)), 2)

    val sortedRdd = rdd.repartitionAndSortWithinPartitions(new HashPartitioner(2))
    assert(sortedRdd.partitioner.isDefined)
  }
}