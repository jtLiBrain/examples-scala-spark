package com.jtLiBrain.examples.spark.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.jtLiBrain.examples.spark.sql.test.SQLTestData
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class DataFrameFunctionsExample extends FunSuite with SQLTestData with DataFrameSuiteBase {
  test("sts and stddev_pop") {
    val sparkSession = spark
    import sparkSession.implicits._

    Seq(
      1
    ).toDF("value")
      .groupBy("value").agg(
      mean("value"),
      stddev("value"), // 样本标准差，自由度为n-1
      stddev_pop("value") // 总体标准差，自由度为n
    ).show(10, false)
  }

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

  test("explode - 空值") {
    val sparkSession = spark
    import sparkSession.implicits._

    val df1 = Seq(
      ("1", "1,10"),
      ("2", "2,20"),
      ("3", null)
    ).toDF("key", "value")
    .select($"key", split($"value", ",").alias("values"))

    df1.select($"key", explode($"values")).show(false)
    /*
    +---+---+
    |key|col|
    +---+---+
    |1  |1  |
    |1  |10 |
    |2  |2  |
    |2  |20 |
    +---+---+
     */

    val df2 = Seq(
      ("1", "1,10"),
      ("2", "2,20"),
      ("3", "")
    ).toDF("key", "value")
      .select($"key", split($"value", ",").alias("values"))

    df2.select($"key", explode($"values")).show(false)
    /*
    +---+---+
    |key|col|
    +---+---+
    |1  |1  |
    |1  |10 |
    |2  |2  |
    |2  |20 |
    |3  |   |
    +---+---+
     */
  }

  test("explode_outer - 空值") {
    val sparkSession = spark
    import sparkSession.implicits._

    val df1 = Seq(
      ("1", "1,10"),
      ("2", "2,20"),
      ("3", null)
    ).toDF("key", "value")
      .select($"key", split($"value", ",").alias("values"))

    df1.select($"key", explode_outer($"values")).show(false)
    /*
+---+----+
|key|col |
+---+----+
|1  |1   |
|1  |10  |
|2  |2   |
|2  |20  |
|3  |null|
+---+----+
     */

    val df2 = Seq(
      ("1", "1,10"),
      ("2", "2,20"),
      ("3", "")
    ).toDF("key", "value")
      .select($"key", split($"value", ",").alias("values"))

    df2.select($"key", explode_outer($"values")).show(false)
    /*
+---+---+
|key|col|
+---+---+
|1  |1  |
|1  |10 |
|2  |2  |
|2  |20 |
|3  |   |
+---+---+
     */
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

  test("describe") {
    val sparkSession = spark
    import sparkSession.implicits._

    Seq(
      1, 2, 2
    ).toDF("value")
      .describe()
      .show()
  }

  test("udf") {
    val a = (1, 2)
    val sparkSession = spark
    import sparkSession.implicits._
    val testUdf = udf {(n: Int) =>
      Map(
        "c1" -> "n",
        "c2" -> "a",
        "c3" -> "b"
      )
    }
    Seq(
      1, 2, 2
    ).toDF("value")
      .select(
        testUdf($"value").alias("u")
      )
    .select(
      $"u.c1",
      $"u.c2",
      $"u.c3"
    )
      .show()
  }
}
