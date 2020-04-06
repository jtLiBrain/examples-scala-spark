package com.jtLiBrain.examples.spark.ml.feature.transformers

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.scalatest.FunSuite

class IndexToStringExample extends FunSuite with DataFrameSuiteBase {
  test("") {
    val df = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")

    // 1.
    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)
    val indexed = indexer.transform(df)

    println(s"将字符串列：'${indexer.getInputCol}' " + s"转换为索引列：'${indexer.getOutputCol}'")
    indexed.show()

    val inputColSchema = indexed.schema(indexer.getOutputCol)
    println(s"StringIndexer将标签存放在输出列的元数据中：" + s"${Attribute.fromStructField(inputColSchema).toString}\n")

    // 2.
    val converter = new IndexToString()
      .setInputCol("categoryIndex")
      .setOutputCol("originalCategory")

    val converted = converter.transform(indexed)

    println(s"使用元数据中的标签，将索引列：'${converter.getInputCol}'转换回原始字符串列：")
    converted.select("id", "categoryIndex", "originalCategory").show()
  }
}
