package com.jtLiBrain.examples.spark.elasticsearch

import org.elasticsearch.spark.rdd.{EsSpark, Metadata}
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

class RDDWithESExample extends FunSuite with SharedSparkContext {
  private val esOptions = Map(
    "es.nodes" -> "localhost",
    "es.port" -> "9200"
  )

  test("saveToEs - as map") {
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

    val rddData = Seq(numbers, airports)

    val rdd = sc.makeRDD(rddData)


    EsSpark.saveToEs(rdd, "spark-es/docs", esOptions)
  }

  test("saveToEs - as case class") {
    val upcomingTrip = Trip("OTP", "SFO")
    val lastWeekTrip = Trip("MUC", "OTP")

    val rddData = Seq(upcomingTrip, lastWeekTrip)

    val rdd = sc.makeRDD(rddData)

    EsSpark.saveToEs(rdd, "spark-es/docs", esOptions)
  }

  test("saveToEs - as json") {
    val json1 = """{"reason" : "business", "airport" : "SFO"}"""
    val json2 = """{"participants" : 5, "airport" : "OTP"}"""

    val rddData = Seq(json1, json2)

    val rdd = sc.makeRDD(rddData)

    EsSpark.saveJsonToEs(rdd, "spark-es/docs", esOptions)
  }

  test("saveToEs - to multi-resources") {
    val game = Map("media_type"->"game", "title" -> "FF VI", "year" -> "1994")
    val book = Map("media_type" -> "book","title" -> "Harry Potter","year" -> "2010")
    val cd = Map("media_type" -> "music","title" -> "Surfing With The Alien")

    val rddData = Seq(game, book, cd)

    val rdd = sc.makeRDD(rddData)

    EsSpark.saveToEs(rdd, "spark-es-media-{media_type}/doc", esOptions)
  }

  test("saveToEsWithMeta - 1") {
    val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
    val muc = Map("iata" -> "MUC", "name" -> "Munich")
    val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

    val rddData = Seq(
      (1 -> otp),
      (2 -> muc),
      (3 -> sfo)
    )

    val rdd = sc.makeRDD(rddData)

    EsSpark.saveToEsWithMeta(rdd, "spark-es-airports", esOptions)
  }

  test("saveToEsWithMeta - 2") {
    val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
    val muc = Map("iata" -> "MUC", "name" -> "Munich")
    val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

    val otpMeta = Map(Metadata.ID -> 1, Metadata.TTL -> "3h")
    val mucMeta = Map(Metadata.ID -> 2, Metadata.VERSION -> "23")
    val sfoMeta = Map(Metadata.ID -> 3)

    val rddData = Seq(
      (otpMeta -> otp),
      (mucMeta -> muc),
      (sfoMeta -> sfo)
    )

    val rdd = sc.makeRDD(rddData)

    EsSpark.saveToEsWithMeta(rdd, "spark-es-airports", esOptions)
  }


  test("read from ES") {
    val rdd = EsSpark.esRDD(sc, "spark-es-airports")

    rdd.collect().foreach{ case (k, v) =>
      println(s"key: $k \t value:$v")
    }
  }


  test("read from ES as JSON") {
    val rdd = EsSpark.esJsonRDD(sc, "spark-es-airports")

    rdd.collect().foreach{ case (k, v) =>
      println(s"key: $k \t value:$v")
    }
  }

}

case class Trip(departure: String, arrival: String)
