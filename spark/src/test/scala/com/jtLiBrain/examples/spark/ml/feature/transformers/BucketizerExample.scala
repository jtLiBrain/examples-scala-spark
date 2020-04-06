package com.jtLiBrain.examples.spark.ml.feature.transformers

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.feature.Bucketizer
import org.scalatest.FunSuite

class BucketizerExample extends FunSuite with DataFrameSuiteBase {
  test("") {
    // case 1
    val data = Array(-999.9, -0.5, -0.3, 0.0, 0.2, 999.9)
    val dataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

    val bucketizer = new Bucketizer()
      .setInputCol("features")
      .setOutputCol("bucketedFeatures")
      .setSplits(splits)

    // 将原始数据转换到其分桶的索引
    val bucketedData = bucketizer.transform(dataFrame)

    println(s"Bucketizer的输出有${bucketizer.getSplits.length-1}个分桶")
    bucketedData.show()


    // case 2
    val data2 = Array(
    (-999.9, -999.9),
    (-0.5, -0.2),
    (-0.3, -0.1),
    (0.0, 0.0),
    (0.2, 0.4),
    (999.9, 999.9))
    val dataFrame2 = spark.createDataFrame(data2).toDF("features1", "features2")

    val splitsArray = Array(
      Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity),
      Array(Double.NegativeInfinity, -0.3, 0.0, 0.3, Double.PositiveInfinity))

    val bucketizer2 = new Bucketizer()
      .setInputCols(Array("features1", "features2"))
      .setOutputCols(Array("bucketedFeatures1", "bucketedFeatures2"))
      .setSplitsArray(splitsArray)

    // 将原始数据转换到其分桶的索引
    val bucketedData2 = bucketizer2.transform(dataFrame2)

    println(s"对于每一个输入列，Bucketizer的输出分别有[" +
      s"${bucketizer2.getSplitsArray(0).length-1}, " +
      s"${bucketizer2.getSplitsArray(1).length-1}] 个分桶")
    bucketedData2.show()
  }
}
