package com.jtLiBrain.examples.spark

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

package object utils {
  implicit class SparkContextFunctions(sc: SparkContext) extends Serializable {
    def collectAsPartitions[T: ClassTag](rdd: RDD[T]): Array[(Int, Array[T])] = {
      val num = rdd.getNumPartitions

      val result = collection.mutable.Buffer[(Int, Array[T])]()

      for(n <- 0 to num-1) {
        val tmp =
        sc.runJob (
          rdd,
          {iter: Iterator[T] => iter.toArray},
          Seq(n)
        )
        result += n -> tmp(0)
      }

      result.toArray
    }
  }

  implicit class SparkRDDFunctions[T : ClassTag](rdd: RDD[T]) extends Serializable {
    def printAsPartitions(): Unit = {
      rdd.foreachPartition{ iter =>
        val partId = TaskContext.getPartitionId()
        println(s"Partition: $partId\n    " + iter.mkString(", "))
      }
    }
  }
}
