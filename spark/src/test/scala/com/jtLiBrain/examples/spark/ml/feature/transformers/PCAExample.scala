package com.jtLiBrain.examples.spark.ml.feature.transformers

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSuite

class PCAExample extends FunSuite with DataFrameSuiteBase {
  test("") {
    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3) // 主成分的个数，也即目标向量空间的维度
      .fit(df)

    val result = pca.transform(df)
    result.show(false)
  }
}
