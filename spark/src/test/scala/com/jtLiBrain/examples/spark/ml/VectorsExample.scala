package com.jtLiBrain.examples.spark.ml

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.Summarizer.{mean, metrics, variance}
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

  test("summarizer") {
    import spark.implicits._

    val data = Seq(
      (Vectors.dense(2.0, 3.0, 5.0), 1.0),
      (Vectors.dense(4.0, 6.0, 7.0), 2.0)
    )

    // [2.0, 4.0] mean:3.0    variance: 2.0 = (2.0-3.0)^2/(2-1) + (4.0-3.0)^2/(2-1)
    // [3.0, 6.0] mean:4.5    variance: 4.5 = (3.0-4.5)^2/(2-1) + (6.0-4.5)^2/(2-1)
    // [5.0, 7.0] mean:6.0    variance: 2.0 = (5.0-6.0)^2/(2-1) + (7.0-6.0)^2/(2-1)

    val df = data.toDF("features", "weight")

    val (meanVal, varianceVal) = df.select(
      metrics("mean", "variance").summary($"features", $"weight").as("summary")
    ).select(
      "summary.mean", "summary.variance"
    ).as[(Vector, Vector)].first()

    println(s"with weight: mean = ${meanVal}, variance = ${varianceVal}")

    val (meanVal2, varianceVal2) = df.select(
      mean($"features"), variance($"features") // 这里是样本方差，不是总体方差
    ).as[(Vector, Vector)].first()

    println(s"without weight: mean = ${meanVal2}, variance = ${varianceVal2}")
  }

}
