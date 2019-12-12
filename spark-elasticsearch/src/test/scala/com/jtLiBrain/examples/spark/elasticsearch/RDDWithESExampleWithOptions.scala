package com.jtLiBrain.examples.spark.elasticsearch

import org.elasticsearch.spark.rdd.{EsSpark, Metadata}
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

class RDDWithESExampleWithOptions extends FunSuite with SharedSparkContext {
  private val esOptions = Map(
    "es.nodes" -> "10.12.6.64:9200",
    "es.net.http.auth.user" -> "elastic",
    "es.net.http.auth.pass" -> "elastic"
  )

  test("saveToEs - with user & pass") {
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

    val rddData = Seq(numbers, airports)

    val rdd = sc.makeRDD(rddData)


    EsSpark.saveToEs(rdd, "ba-jt-test/docs", esOptions)
  }
}