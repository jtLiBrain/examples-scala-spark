package com.jtLiBrain.examples.spark.ml

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSuite

class VectorsExample extends FunSuite with DataFrameSuiteBase {
  test("dense") {
    val vector = Vectors.dense(0.0, 10.0, 0.5)

    println(vector.toString)
  }

  test("sparse") {
    // 创建一个有4个元素的向量，
    // 其中该向量的索引为：[0,2,3]，相应索引对应的数值为：[0.0,2.0,3.0]
    val vector = Vectors.sparse(4, Seq(
      (0, 0.0),
      (2, 2.0),
      (3, 3.0)
    ))

    println(vector.toString)
  }

}
