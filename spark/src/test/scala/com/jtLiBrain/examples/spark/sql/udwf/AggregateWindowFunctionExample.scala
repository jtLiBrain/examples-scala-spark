package com.jtLiBrain.examples.spark.sql.udwf

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.jtLiBrain.examples.spark.sql.test.SQLTestData
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class AggregateWindowFunctionExample extends FunSuite with DataFrameSuiteBase with SQLTestData {
  test("1") {
    val sparkSession = spark
    import sparkSession.implicits._

    val specs = Window.partitionBy($"user").orderBy($"ts".asc)

    val res = userActivityData.select(
        $"user",
        from_unixtime($"ts"/1000).alias("time"),
        $"ts",
        $"session"
      )
      .withColumn(
        "newsession",
        SQLUtil.calculateSession($"ts", $"session") over specs  // window duration in ms
      )

    res.show(10, 200)
  }
}
