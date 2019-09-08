package com.jtLiBrain.examples.spark.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.jtLiBrain.examples.spark.sql.test.SQLTestData
import org.scalatest.FunSuite

/**
  * NOTE: 关于在流式计算中使用 FlatMapGroupsWithState，在Spark源码包中的：
  * FlatMapGroupsWithStateSuite.testWithAllStateVersions("flatMapGroupsWithState - streaming w/ event time timeout + watermark")
  * 给出了很好的例子
 */
class StreamDataFrameFlatMapGroupsWithStateExample extends FunSuite with SQLTestData with DataFrameSuiteBase {
}
