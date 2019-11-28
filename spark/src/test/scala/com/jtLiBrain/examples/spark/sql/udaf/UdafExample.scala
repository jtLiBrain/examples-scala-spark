package com.jtLiBrain.examples.spark.sql.udaf

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite

class UdafExample extends FunSuite with DataFrameSuiteBase {
  /**
    * this test can't pass
    */
  test("udaf - bufferSchema is an mutable map") {
    val sparkSession = spark
    import sparkSession.implicits._

    val agg = new AggregatorWithMutableMap()

    val df = Seq(
      Map("a" -> 1L, "b" -> 1L),
      Map("a" -> 2L, "b" -> 1L),
      Map("a" -> 2L)
    )
    .toDF("mv")
    .select(agg($"mv"))

    df.printSchema()
    df.show()
  }

  test("udaf - bufferSchema is an immutable map") {
    val sparkSession = spark
    import sparkSession.implicits._

    val agg = new AggregatorWithImmutableMap()

    val df = Seq(
      Map("a" -> 1L, "b" -> 1L),
      Map("a" -> 2L, "b" -> 1L),
      Map("a" -> 2L)
    )
      .toDF("mv")
      .select(agg($"mv"))

    df.printSchema()
    df.show()
  }

  /**
    * this test can't pass
    */
  test("udaf - bufferSchema is an object") {
    val sparkSession = spark
    import sparkSession.implicits._

    val agg = new AggregatorWithObject()

    val df = Seq(
      Map("a" -> 1L, "b" -> 1L),
      Map("a" -> 2L, "b" -> 1L),
      Map("a" -> 2L)
    )
      .toDF("mv")
      .select(agg($"mv"))

    df.printSchema()
    df.show()
  }
}
