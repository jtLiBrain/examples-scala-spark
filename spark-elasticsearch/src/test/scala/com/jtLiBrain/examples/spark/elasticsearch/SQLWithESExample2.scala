package com.jtLiBrain.examples.spark.elasticsearch

import org.elasticsearch.spark.sql.EsSparkSQL
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions.regexp_replace
import org.scalatest.FunSuite

class SQLWithESExample2 extends FunSuite with DataFrameSuiteBase {
  test("saveToEs - array as string") {
    val sparkSession = spark
    import sparkSession.implicits._

    val df = Seq(
      ("1", Array("1", "10")),
      ("2", Array("2", "20")),
      ("3", Array("3", "30"))
    ).toDF("key", "values")

    val esOptions = Map(
      "es.nodes" -> "192.168.56.101:9200"
    )

    EsSparkSQL.saveToEs(df, "spark-es-sql-datatype-array", esOptions)
  }
  test("saveToEs - date as string") {
    val sparkSession = spark
    import sparkSession.implicits._

    val df = Seq(
      ("2019-11-06T01:47:01.528+08:00")
    ).toDF("timeStr")
    /*.withColumn(
      "time",
      regexp_replace($"timeStr", "T", " ").cast("timestamp")
    )*/

    val esOptions = Map(
      "es.nodes" -> "192.168.56.101:9200"
    )

    EsSparkSQL.saveToEs(df, "spark-es-sql-datatype-date", esOptions)
  }
}
