package com.jtLiBrain.examples.spark.sql.aggregator

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator
import org.scalatest.FunSuite

import scala.collection.mutable

/**
  * see org.apache.spark.examples.sql.UserDefinedTypedAggregation
  *
  *
  * NOTE: can't work
  */
class AggregatorExample extends FunSuite with DataFrameSuiteBase {
  test("udaf - bufferSchema is an mutable map") {
    val sparkSession = spark
    import sparkSession.implicits._

    val aggV = MyAggregator.toColumn.name("aggV")

    val df = Seq(
      Map("a" -> 1L, "b" -> 1L),
      Map("a" -> 2L, "b" -> 1L),
      Map("a" -> 2L)
    )
      .toDF("mv")
      .select(aggV)

    df.printSchema()
    df.show()
  }
}


object MyAggregator extends Aggregator[Map[String, Long], mutable.Map[String, Long], mutable.Map[String, Long]] {
  override def zero: mutable.Map[String, Long] = mutable.Map[String, Long]()

  override def reduce(b: mutable.Map[String, Long], a: Map[String, Long]): mutable.Map[String, Long] = {
    mergeMap(a, b)

    b
  }

  override def merge(b1: mutable.Map[String, Long], b2: mutable.Map[String, Long]): mutable.Map[String, Long] = {
    mergeMap(b1, b2)

    b1
  }

  override def finish(reduction: mutable.Map[String, Long]): mutable.Map[String, Long] = {
    reduction
  }

  override def bufferEncoder: Encoder[mutable.Map[String, Long]] = Encoders.kryo[mutable.Map[String, Long]]

  override def outputEncoder: Encoder[mutable.Map[String, Long]] = Encoders.kryo[mutable.Map[String, Long]]

  def mergeMap(srcMap: scala.collection.Map[String, Long], targetMap: collection.mutable.Map[String, Long]): Unit = {
    srcMap.foreach{ case (k,v) =>
      val valueInBuffer = targetMap.get(k).getOrElse(0L)

      targetMap += k -> {v+valueInBuffer}
    }
  }
}