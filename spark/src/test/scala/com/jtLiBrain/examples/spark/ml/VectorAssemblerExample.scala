package com.jtLiBrain.examples.spark.ml

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSuite

class VectorAssemblerExample extends FunSuite with DataFrameSuiteBase {
  test("VectorAssembler") {
    val dataset = spark.createDataFrame(
      Seq(
        (1, 2, 3.0, Vectors.dense(4.0, 5.0, 0.6), 7.0)
      )
    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")

    val assembler = new VectorAssembler()
      .setInputCols(Array("hour", "mobile", "userFeatures"))
      .setOutputCol("features")

    val output = assembler.transform(dataset)

    println("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")

    output.select("features", "clicked")
      .show(false)
  }


}
