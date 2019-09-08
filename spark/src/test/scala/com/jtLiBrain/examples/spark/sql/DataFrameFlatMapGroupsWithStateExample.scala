package com.jtLiBrain.examples.spark.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.jtLiBrain.examples.spark.sql.test.SQLTestData
import com.jtLiBrain.examples.spark.sql.test.SQLTestData.CourseSales
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.scalatest.FunSuite

case class RunningStat(lastYear:Int, var count: Long, var earnings: Double) {
  override def toString: String = s"lastYear: $lastYear, count: $count,  earnings: $earnings"
}

/**
  * Jiantao Li
  */
class DataFrameFlatMapGroupsWithStateExample extends FunSuite with SQLTestData with DataFrameSuiteBase {
  test("flatMapGroupsWithState - batch") {
    val sparkSession = spark
    import sparkSession.implicits._

    val stateFunc = (year: Int, courseSalesRecord: Iterator[CourseSales], state: GroupState[RunningStat]) => {
      if (state.exists) throw new IllegalArgumentException("state.exists should be false")

      println("-------------- start")

      println(s"Current year is: $year")
      println(s"Current course sales record are: ")

      // NOTE courseSalesRecord is Iterator, so it can only be traversed once
      val result = courseSalesRecord.foldLeft(Array(0, 0.0D)) { case (combiner, courseSales) =>

        println(s"\t$courseSales")

        Array(combiner(0) + 1, combiner(1) + courseSales.earnings)
      }

      state.update(RunningStat(year, result(0).toLong, result(1)))
      println("-------------- end")
      Iterator((year, result(0).toLong, result(1)))
    }

    val result = courseSales.as[CourseSales]
      .groupByKey(x => x.year)
      .flatMapGroupsWithState(OutputMode.Update(), GroupStateTimeout.NoTimeout())(stateFunc)
      .toDF()

    courseSales.show(100)
    result.show(100)
  }
}
