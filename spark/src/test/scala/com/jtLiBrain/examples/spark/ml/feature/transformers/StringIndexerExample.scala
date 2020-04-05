package com.jtLiBrain.examples.spark.ml.feature.transformers

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.feature.StringIndexer
import org.scalatest.FunSuite

class StringIndexerExample extends FunSuite with DataFrameSuiteBase {
  test("") {
    val baseDF = spark.createDataFrame(
      Seq(
        (0, "a"),
        (1, "b"),
        (2, "c"),
        (3, "a"),
        (4, "a"),
        (5, "c")
      )
    ).toDF("id", "category")

    val df1 = spark.createDataFrame(
      Seq(
        (0, "a"),
        (1, "b")
      )
    ).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val model = indexer.fit(baseDF)

    model.transform(baseDF).show(false)
    model.transform(df1).show(false)
  }
}
