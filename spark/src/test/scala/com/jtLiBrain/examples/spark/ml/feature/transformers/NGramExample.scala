package com.jtLiBrain.examples.spark.ml.feature.transformers

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.feature.NGram
import org.scalatest.FunSuite

class NGramExample extends FunSuite with DataFrameSuiteBase {
  test("") {
    val wordDataFrame = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat"))
    )).toDF("id", "words")

    val ngram = new NGram()
      .setN(2)
      .setInputCol("words")
      .setOutputCol("ngrams")

    val ngramDataFrame = ngram.transform(wordDataFrame)
    ngramDataFrame.show(false)
    /*
+---+------------------------------------------+------------------------------------------------------------------+
|id |words                                     |ngrams                                                            |
+---+------------------------------------------+------------------------------------------------------------------+
|0  |[Hi, I, heard, about, Spark]              |[Hi I, I heard, heard about, about Spark]                         |
|1  |[I, wish, Java, could, use, case, classes]|[I wish, wish Java, Java could, could use, use case, case classes]|
|2  |[Logistic, regression, models, are, neat] |[Logistic regression, regression models, models are, are neat]    |
+---+------------------------------------------+------------------------------------------------------------------+
     */
  }
}
