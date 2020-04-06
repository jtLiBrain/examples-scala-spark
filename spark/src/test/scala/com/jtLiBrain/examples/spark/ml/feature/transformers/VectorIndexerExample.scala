package com.jtLiBrain.examples.spark.ml.feature.transformers

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.feature.VectorIndexer
import org.scalatest.FunSuite

class VectorIndexerExample extends FunSuite with DataFrameSuiteBase {
  test("") {
    val data = spark.read.format("libsvm").load("/Users/Dream/Dev/git/examples-scala-spark/data/mllib/sample_libsvm_data.txt")
    data.printSchema()
    data.show(false)

    val indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(10)

    val indexerModel = indexer.fit(data)

    val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
    println(s"选取了 ${categoricalFeatures.size} 个类别型特征：" + s"${categoricalFeatures.mkString(", ")}")

    // Create new column "indexed" with categorical values transformed to indices
    val indexedData = indexerModel.transform(data)
    indexedData.show()
  }
}
