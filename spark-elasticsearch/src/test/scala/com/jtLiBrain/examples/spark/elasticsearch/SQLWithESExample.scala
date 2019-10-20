package com.jtLiBrain.examples.spark.elasticsearch

import org.elasticsearch.spark.sql.EsSparkSQL
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite

case class Person(name: String, age: Int)

class SQLWithESExample extends FunSuite with DataFrameSuiteBase {
  test("saveToEs") {
    val sparkSession = spark
    import sparkSession.implicits._

    val df = sparkSession.read.textFile("file:/Users/Dream/Dev/git/examples-scala-spark/data/people.txt")
        .map(_.split(","))
        .map(p => Person(p(0), p(1).trim.toInt))
        .toDF()

    EsSparkSQL.saveToEs(df, "sparks-es-sql")
  }

  test("read from es - 1") {
    val sparkSession = spark

    val df = sparkSession.read.format("es")
        .load("sparks-es-sql")

    df.show(10)
  }

  test("read from es - 2") {
    val sparkSession = spark

    sparkSession.sql(
        "CREATE TEMPORARY VIEW myIndex " +
        "USING org.elasticsearch.spark.sql " +
        "OPTIONS (resource 'sparks-es-sql')"
    )

    val df = sparkSession.sql("SELECT * FROM myIndex")

    df.show(10)
  }

  test("read from es - 3") {
    val sparkSession = spark

    val df = EsSparkSQL.esDF(sparkSession, "sparks-es-sql")

    df.show(10)
  }

  test("read from es - only include some fields") {
    val esOptions = Map(
      "es.read.field.include" -> "name"
    )

    val sparkSession = spark

    val df = EsSparkSQL.esDF(sparkSession, "sparks-es-sql", esOptions)

    df.show(10)
  }
}
