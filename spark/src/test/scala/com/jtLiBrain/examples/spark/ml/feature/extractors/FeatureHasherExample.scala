package com.jtLiBrain.examples.spark.ml.feature.extractors

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.feature.FeatureHasher
import org.scalatest.FunSuite

class FeatureHasherExample extends FunSuite with DataFrameSuiteBase {
  test("") {
    val dataset = spark.createDataFrame(Seq(
      (2.2, true, "1", "foo"),
      (3.3, false, "2", "bar"),
      (4.4, false, "3", "baz"),
      (5.5, false, "4", "foo")
    )).toDF("real", "bool", "stringNum", "string")

    val hasher = new FeatureHasher()
      .setInputCols("real", "bool", "stringNum", "string")
      .setOutputCol("features")

    val featurized = hasher.transform(dataset)
    featurized.show(false)

    /*
+----+-----+---------+------+--------------------------------------------------------+
|real|bool |stringNum|string|features                                                |
+----+-----+---------+------+--------------------------------------------------------+
|2.2 |true |1        |foo   |(262144,[174475,247670,257907,262126],[2.2,1.0,1.0,1.0])|
|3.3 |false|2        |bar   |(262144,[70644,89673,173866,174475],[1.0,1.0,1.0,3.3])  |
|4.4 |false|3        |baz   |(262144,[22406,70644,174475,187923],[1.0,1.0,4.4,1.0])  |
|5.5 |false|4        |foo   |(262144,[70644,101499,174475,257907],[1.0,1.0,5.5,1.0]) |
+----+-----+---------+------+--------------------------------------------------------+
     */
  }
}
