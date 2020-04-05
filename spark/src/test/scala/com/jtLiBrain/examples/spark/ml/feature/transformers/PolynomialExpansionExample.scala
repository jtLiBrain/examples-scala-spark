package com.jtLiBrain.examples.spark.ml.feature.transformers

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSuite

class PolynomialExpansionExample extends FunSuite with DataFrameSuiteBase {
  test("") {
    val data = Array(
      Vectors.dense(2.0, 1.0),
      Vectors.dense(0.0, 0.0),
      Vectors.dense(3.0, -1.0)
    )
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val polyExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")
      .setDegree(3) // 几次多项式

    val polyDF = polyExpansion.transform(df)
    polyDF.show(false)
  }
}
