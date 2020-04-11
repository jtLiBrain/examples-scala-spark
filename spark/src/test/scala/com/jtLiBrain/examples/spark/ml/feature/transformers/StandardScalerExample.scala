package com.jtLiBrain.examples.spark.ml.feature.transformers

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.feature.StandardScaler
import org.scalatest.FunSuite

class StandardScalerExample extends FunSuite with DataFrameSuiteBase {
  test("") {
    val dataFrame = spark.read.format("libsvm").load("E:\\me\\codebase\\examples-scala-spark\\spark\\data\\sample_libsvm_data.txt")

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    // 通过拟合StandardScaler来计算汇总统计数据
    val scalerModel = scaler.fit(dataFrame)

    // 使用标准差单位对每个特征进行标准化
    val scaledData = scalerModel.transform(dataFrame)
    scaledData.show()
  }
}
