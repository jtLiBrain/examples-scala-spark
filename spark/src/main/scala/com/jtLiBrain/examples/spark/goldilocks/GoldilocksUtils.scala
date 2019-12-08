package com.jtLiBrain.examples.spark.goldilocks

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object GoldilocksUtils {
  /**
    *
    * For example:
    *
    * dataframe:
    *   (0.0, 4.5, 7.7, 5.0)
    *   (1.0, 5.5, 6.7, 6.0)
    *
    *
    * The output will be:
    *   (0, 0.0)
    *   (1, 4.5)
    *   (2, 7.7)
    *   (3, 5.0)
    *   (0, 1.0)
    *   (1, 5.5)
    *   (2, 6.7)
    *   (3, 6.0)
    */
  def mapToKeyValuePairs(dataFrame: DataFrame): RDD[(Int, Double)] = {
    val rowLength = dataFrame.schema.length
    dataFrame.rdd.flatMap(
      row => Range(0, rowLength).map(i => (i, row.getDouble(i)))
    )
  }
}
