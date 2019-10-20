package com.jtLiBrain.examples.spark.elasticsearch

import org.elasticsearch.spark.streaming.EsSparkStreaming
import com.holdenkarau.spark.testing.StreamingActionBase
import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.spark.rdd.Metadata
import org.scalatest.FunSuite

class StreamingWithESExample extends FunSuite with StreamingActionBase {
  test("saveToEs") {
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

    val rddData = Seq(Seq(numbers, airports))

    val actionFun = (in: DStream[Object]) => {
      EsSparkStreaming.saveToEs(in, "sparks-es-streaming")
    }

    runAction(rddData, actionFun)
  }

  test("saveToEs - as case class") {
    val upcomingTrip = Trip("OTP", "SFO")
    val lastWeekTrip = Trip("MUC", "OTP")

    val rddData = Seq(Seq(upcomingTrip, lastWeekTrip))

    val actionFun = (in: DStream[Object]) => {
      EsSparkStreaming.saveToEs(in, "sparks-es-streaming")
    }

    runAction(rddData, actionFun)
  }

  test("saveToEs - as json") {
    val json1 = """{"reason" : "business", "airport" : "SFO"}"""
    val json2 = """{"participants" : 5, "airport" : "OTP"}"""

    val rddData = Seq(Seq(json1, json2))

    val actionFun = (in: DStream[Object]) => {
      EsSparkStreaming.saveToEs(in, "sparks-es-streaming")
    }

    runAction(rddData, actionFun)
  }

  test("saveToEs - to multi-resources") {
    val game = Map("media_type"->"game", "title" -> "FF VI", "year" -> "1994")
    val book = Map("media_type" -> "book","title" -> "Harry Potter","year" -> "2010")
    val cd = Map("media_type" -> "music","title" -> "Surfing With The Alien")

    val rddData = Seq(Seq(game, book, cd))

    val actionFun = (in: DStream[Object]) => {
      EsSparkStreaming.saveToEs(in, "sparks-es-streaming")
    }

    runAction(rddData, actionFun)
  }

  test("saveToEsWithMeta - 1") {
    val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
    val muc = Map("iata" -> "MUC", "name" -> "Munich")
    val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

    val rddData = Seq(
      Seq(
        (1 -> otp),
        (2 -> muc),
        (3 -> sfo)
      )
    )

    val actionFun = (in: DStream[(Any, Object)]) => {
      EsSparkStreaming.saveToEsWithMeta(in, "sparks-es-streaming")
    }

    runAction(rddData, actionFun)
  }

  test("saveToEsWithMeta - 2") {
    val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
    val muc = Map("iata" -> "MUC", "name" -> "Munich")
    val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

    val otpMeta = Map(Metadata.ID -> 1, Metadata.TTL -> "3h")
    val mucMeta = Map(Metadata.ID -> 2, Metadata.VERSION -> "23")
    val sfoMeta = Map(Metadata.ID -> 3)

    val rddData = Seq(
      Seq(
        (otpMeta -> otp),
        (mucMeta -> muc),
        (sfoMeta -> sfo)
      )
    )

    val actionFun = (in: DStream[(Any, Object)]) => {
      EsSparkStreaming.saveToEsWithMeta(in, "sparks-es-streaming")
    }

    runAction(rddData, actionFun)
  }

}
