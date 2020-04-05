package com.jtLiBrain.examples.spark.ml.feature.transformers

import java.util.Locale

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.feature.StopWordsRemover
import org.scalatest.FunSuite

class StopWordsRemoverExample extends FunSuite with DataFrameSuiteBase {
  test("") {
    Locale.setDefault(Locale.ENGLISH)

    val dataSet = spark.createDataFrame(Seq(
    (0, Seq("I", "saw", "the", "red", "balloon")),
    (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")


    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")

    val resultDF = remover.transform(dataSet)

    resultDF.printSchema()
    resultDF.show(false)
  }
}
