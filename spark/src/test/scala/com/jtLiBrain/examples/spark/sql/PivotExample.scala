package com.jtLiBrain.examples.spark.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class PivotExample extends FunSuite with DataFrameSuiteBase {
  test("pivot") {
    val sparkSession = spark
    import sparkSession.implicits._

    val rawDF = sparkSession.read.option("header", "true")
      .csv("examples/data/Observed_Monthly_Rain_Gauge_Accumulations_-_Oct_2002_to_May_2017.csv")
      .select(
        year(from_unixtime(unix_timestamp($"Date", "MM/dd/yyyy"))).alias("YEAR"),
        month(from_unixtime(unix_timestamp($"Date", "MM/dd/yyyy"))).alias("MONTH"),
        $"RG01",
        $"RG02"
      ).filter($"YEAR" > 2010 and $"YEAR" < 2017)

    val pivotDF = rawDF.select("YEAR", "MONTH", "RG01")
      .groupBy("YEAR")
      .pivot("MONTH", Seq(
        "1", "2", "3", "4", "5", "6",
        "7", "8", "9", "10", "11", "12"
      )).agg(
      first($"RG01")
    ).orderBy("YEAR")


    rawDF.show(100)
    pivotDF.show(100)
  }
}
