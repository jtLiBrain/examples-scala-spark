package com.jtLiBrain.examples.spark.ml.feature.transformers

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSuite

class ElementwiseProductExample extends FunSuite with DataFrameSuiteBase {
  test("") {
    // Create some vector data; also works for sparse vectors
    val dataFrame = spark.createDataFrame(Seq(
      ("a", Vectors.dense(1.0, 2.0, 3.0)),
      ("b", Vectors.dense(4.0, 5.0, 6.0))
    )).toDF("id", "vector")

    val transformingVector = Vectors.dense(0.0, 1.0, 2.0)

    val transformer = new ElementwiseProduct()
      .setScalingVec(transformingVector)
      .setInputCol("vector")
      .setOutputCol("transformedVector")

    // Batch transform the vectors to create new column:
    transformer.transform(dataFrame).show()
  }
}
