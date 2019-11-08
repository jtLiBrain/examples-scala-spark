package com.jtLiBrain.examples.spark.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.jtLiBrain.examples.spark.sql.test.SQLTestData
import org.apache.spark.sql.functions.{approx_count_distinct, count, countDistinct, explode}
import org.scalatest.FunSuite

class DataFrameFunctionsExample extends FunSuite with SQLTestData with DataFrameSuiteBase {
  test("explode") {
    val sparkSession = spark
    import sparkSession.implicits._

    Seq(
      ("1", Array("1", "10")),
      ("2", Array("2", "20"))
    ).toDF("key", "value")
    .select(
      $"key", explode($"value").alias("value")
    ).show(10, false)
  }

  test("countDistinct") {
    val sparkSession = spark
    import sparkSession.implicits._

    Seq(
      ("1", null),
      ("2", "v2"),
      ("2", "v2")
    ).toDF("key", "values")
      .select(
        count("*"),
        countDistinct("key"),
        countDistinct("values"),
        approx_count_distinct("values")
      ).show(10, false)
  }
}
