package com.jtLiBrain.examples.spark.ml.feature.transformers

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.feature.OneHotEncoderEstimator
import org.scalatest.FunSuite

class OneHotEncoderEstimatorExample extends FunSuite with DataFrameSuiteBase {
  test("") {
    val df = spark.createDataFrame(Seq(
      (0.0, 1.0),
      (1.0, 0.0),
      (2.0, 1.0),
      (0.0, 2.0),
      (0.0, 1.0),
      (2.0, 0.0)
    )).toDF("categoryIndex1", "categoryIndex2")

    /**
     * 将类别索引的列的每个值映射为一个二元向量
     */
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("categoryIndex1", "categoryIndex2"))
      .setOutputCols(Array("categoryVec1", "categoryVec2"))

    val model = encoder.fit(df)

    val encoded = model.transform(df)
    encoded.printSchema()
    encoded.show()
    /*
    对照输出结果，参看OneHotEncoderEstimator的文档

可以看到每个稀疏向量categoryVec1的只给定了值为1.0和其对应的索引
+--------------+-------------+
|categoryIndex1| categoryVec1|
+--------------+-------------+
|           0.0|(2,[0],[1.0])|
|           1.0|(2,[1],[1.0])|
|           2.0|    (2,[],[])|
|           0.0|(2,[0],[1.0])|
|           0.0|(2,[0],[1.0])|
|           2.0|    (2,[],[])|
+--------------+-------------+

+--------------+--------------+-------------+-------------+
|categoryIndex1|categoryIndex2| categoryVec1| categoryVec2|
+--------------+--------------+-------------+-------------+
|           0.0|           1.0|(2,[0],[1.0])|(2,[1],[1.0])|
|           1.0|           0.0|(2,[1],[1.0])|(2,[0],[1.0])|
|           2.0|           1.0|    (2,[],[])|(2,[1],[1.0])|
|           0.0|           2.0|(2,[0],[1.0])|    (2,[],[])|
|           0.0|           1.0|(2,[0],[1.0])|(2,[1],[1.0])|
|           2.0|           0.0|    (2,[],[])|(2,[0],[1.0])|
+--------------+--------------+-------------+-------------+
     */
  }
}
