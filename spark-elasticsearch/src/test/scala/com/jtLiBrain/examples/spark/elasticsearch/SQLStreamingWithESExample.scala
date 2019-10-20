package com.jtLiBrain.examples.spark.elasticsearch

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite

/**
  * NOTE that: only compatible with Spark versions 2.2.0 and onward
  */
class SQLStreamingWithESExample extends FunSuite with DataFrameSuiteBase {
  test("save to ES") {
    val sparkSession = spark
    import sparkSession.implicits._

    val df = sparkSession.readStream.textFile("file:/Users/Dream/Dev/git/examples-scala-spark/data/*.txt")
      .map(_.split(","))
      .map(p => Person(p(0), p(1).trim.toInt))

    val query = df.writeStream.format("es")
        .start("sparks-es-sql-streaming")

    query.awaitTermination(10000L)
  }

  /**
    * see https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html#spark-sql-streaming-commit-log
    */
  test("save to ES with checkpoint location") {

  }
}
