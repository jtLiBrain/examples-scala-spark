package com.jtLiBrain.examples.spark.sql.window

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.jtLiBrain.examples.spark.sql.test.SQLTestData
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{dense_rank, max}
import org.scalatest.FunSuite

class WindowExample extends FunSuite with DataFrameSuiteBase with SQLTestData {
  val data1 = Seq(
    ("Thin",        "Cell phone", 6000),
    ("Normal",      "Tablet",     1500),
    ("Mini",        "Tablet",     5500),
    ("Ultra thin",  "Cell phone", 5500),
    ("Very thin",   "Cell phone", 6000),
    ("Big",         "Tablet",     2500),
    ("Bendable",    "Cell phone", 3000),
    ("Foldable",    "Cell phone", 3000),
    ("Pro",         "Tablet",     4500),
    ("Pro2",        "Tablet",     6500)
  )

  test("1") {
    val sparkSession = spark
    import sparkSession.implicits._

    val df = data1.toDF("product", "category", "revenue")

    val wSpec = Window.partitionBy("category").orderBy($"revenue".desc)

    val resultDF = df.withColumn(
      "rank",
      dense_rank().over(wSpec)
    ).filter($"rank" <= 2)

    resultDF.show(100)
  }

  test("2") {
    val sparkSession = spark
    import sparkSession.implicits._

    val df = data1.toDF("product", "category", "revenue")

    val wSpec = Window.partitionBy("category").orderBy($"revenue".desc)

    val revenue_diff = max("revenue").over(wSpec) - $"revenue"

    val resultDF = df.withColumn(
      "revenue_diff",
      revenue_diff
    )

    resultDF.show(100)
  }
}
