package com.jtLiBrain.examples.spark.ml.feature.extractors

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import org.scalatest.FunSuite

class Word2VecExample extends FunSuite with DataFrameSuiteBase {
  test("") {
    // 1. Input data: Each row is a bag of words from a sentence or document.
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // 2. 基于该文档中所有词的平均值，模型将文档转换成一个固定大小的向量
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)

    // 3. 该向量可以当作特征用于预测、文档相似度计算、等等
    val result = model.transform(documentDF)
    result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
      println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n") }

    // 4.
  }
}
