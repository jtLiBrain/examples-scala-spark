/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jtLiBrain.examples.spark.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.jtLiBrain.examples.spark.sql.test.SQLTestData
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class DataFramePivotSuite extends FunSuite with DataFrameSuiteBase with SQLTestData {
  test("pivot courses") {
    val sparkSession = spark
    import sparkSession.implicits._

    val pivotDF1 = courseSales.groupBy($"year").pivot("course", Seq("dotNET", "Java"))
      .agg(sum($"earnings"))
      .orderBy($"year".asc)

    val pivotDF2 = courseSales.groupBy($"year").pivot("course", Seq("dotNET", "Java"))
      .agg("earnings" -> "sum")
      .orderBy($"year".asc)

    val pivotDF3 = courseSales.groupBy($"year").pivot("course", Seq("dotNET", "Java"))
      .agg(sum($"earnings"), avg($"earnings"))
      .orderBy($"year".asc)

    val pivotDF4 = courseSales.groupBy($"year").pivot("course", Seq("dotNET", "Java"))
      .agg(sum($"earnings").alias("sum"), avg($"earnings").alias("avg"))
      .orderBy($"year".asc)

    courseSales.show(100)
    pivotDF1.show(100)
    pivotDF2.show(100)
    pivotDF3.show(100)
    pivotDF4.show(100)
  }

  /**
    * 分组列的值在一列中列出来，pivot列的值在一行中列出来；
    * 可以认为是先按分组列分组，再按各个pivot列分组
    */
  test("pivot year") {
    val sparkSession = spark
    import sparkSession.implicits._

    val pivotDF = courseSales.groupBy("course").pivot("year", Seq(2012, 2013))
      .agg(sum($"earnings"))

    courseSales.show(100)
    pivotDF.show(100)
  }

  test("pivot courses with no values") {
    val sparkSession = spark
    import sparkSession.implicits._

    val pivotDF = courseSales.groupBy("year").pivot("course")
      .agg(sum($"earnings"))

    courseSales.show(100)
    pivotDF.show(100) // Java列 出现在dotNET前面
  }

  test("pivot year with no values") {
    val sparkSession = spark
    import sparkSession.implicits._

    val pivotDF = courseSales.groupBy("course").pivot("year")
      .agg(sum($"earnings"))

    courseSales.show(100)
    pivotDF.show(100)
  }

  test("optimized pivot planned") {
    val sparkSession = spark
    import sparkSession.implicits._

    val pivotDF1 = courseSales.groupBy("year")
      // pivot with extra columns to trigger optimization
      .pivot("course", Seq("dotNET", "Java") ++ (1 to 10).map(_.toString))
      .agg(sum($"earnings"))

    val pivotDF2 = courseSales.groupBy("year")
      // pivot with extra columns to trigger optimization
      .pivot("course", Seq("dotNET", "Java") ++ (1 to 10).map(_.toString))
      .agg(sum($"earnings"))
      .select("year", "dotNET", "Java")

    courseSales.show(100)
    pivotDF1.show(100)
    pivotDF2.show(100)

    pivotDF1.explain()
    pivotDF2.explain()
  }

  test("optimized pivot with multiple aggregations") {
    val sparkSession = spark
    import sparkSession.implicits._

    val pivotDF1 = courseSales.groupBy($"year")
        // pivot with extra columns to trigger optimization
        .pivot("course", Seq("dotNET", "Java") ++ (1 to 10).map(_.toString))
        .agg(sum($"earnings"), avg($"earnings"))

    courseSales.show(100)
    pivotDF1.show(100)
  }

  test("pivot with datatype not supported by PivotFirst") {
    val sparkSession = spark
    import sparkSession.implicits._
      val pivotDF1 = complexData.groupBy().pivot("b", Seq(true, false))
        .agg(max("a"))

    complexData.show(100)
    pivotDF1.show(100)
  }

  test("pivot with datatype not supported by PivotFirst 2") {
    val sparkSession = spark
    import sparkSession.implicits._

    val pivotDF1 = courseSales.withColumn("e", expr("array(earnings, 7.0d)"))
      .groupBy("year").pivot("course", Seq("dotNET", "Java"))
      .agg(min($"e")) // 为什么这里求的是分组中min，而结果仍然是对新列e中的 earning 求进行了sum呢？

    courseSales.withColumn("e", expr("array(earnings, 7.0d)")).show(100)
    pivotDF1.show(100)
  }


  test("SPARK-24722: pivoting by a constant") {
    val sparkSession = spark
    import sparkSession.implicits._

    val pivotDF1 = trainingSales
      .groupBy($"sales.year")
      .pivot(lit(123), Seq(123))
      .agg(sum($"sales.earnings"))

    trainingSales.show(100, 100)
    pivotDF1.show(100)
  }
}
