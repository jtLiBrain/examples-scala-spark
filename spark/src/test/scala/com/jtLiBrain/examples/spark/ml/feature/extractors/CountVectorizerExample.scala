package com.jtLiBrain.examples.spark.ml.feature.extractors

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.scalatest.FunSuite

class CountVectorizerExample extends FunSuite with DataFrameSuiteBase {
  test("") {
    // 1. 从语料库拟合出CountVectorizerModel
    val df = spark.createDataFrame(Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a"))
    )).toDF("id", "words")

    val cvModel1: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3) // 词库大小
      .setMinDF(2)
      .fit(df)

    // 2.或者，使用一个先验词汇表定义一个CountVectorizerModel
    val cvModel2 = new CountVectorizerModel(Array("a", "b", "c"))
      .setInputCol("words")
      .setOutputCol("features")

    cvModel1.transform(df).show(false)
    cvModel2.transform(df).show(false)
  }

}
