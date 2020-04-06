package com.jtLiBrain.examples.spark.ml.feature.selectors

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.feature.RFormula
import org.scalatest.FunSuite

class RFormulaExample extends FunSuite with DataFrameSuiteBase {
  test("") {
    val dataset = spark.createDataFrame(Seq(
      (7, "US", 18, 1.0),
      (8, "CA", 12, 0.0),
      (9, "NZ", 15, 0.0)
    )).toDF("id", "country", "hour", "clicked")

    val formula = new RFormula()
      .setFormula("clicked ~ country + hour")
      .setFeaturesCol("features")
      .setLabelCol("label")

    val output = formula.fit(dataset).transform(dataset)
    output.select("features", "label").show()
  }
}
