package com.jtLiBrain.examples.spark.goldilocks

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.{Map, mutable}

object GoldilocksV3 {

  /**
    * Find nth target rank for every column.
    *
    * For example:
    *
    * dataframe:
    *   (0.0, 4.5, 7.7, 5.0)
    *   (1.0, 5.5, 6.7, 6.0)
    *   (2.0, 5.5, 1.5, 7.0)
    *   (3.0, 5.5, 0.5, 7.0)
    *   (4.0, 5.5, 0.5, 8.0)
    *
    * ranks:
    *   1, 3
    *
    * The output will be:
    *   0 -> (0.0, 2.0)
    *   1 -> (4.5, 5.5)
    *   2 -> (7.7, 1.5)
    *   3 -> (5.0, 7.0)
    *
    * @param dataFrame dataframe of doubles
    * @param ranks the required ranks for every column
    *
    * @return map of (column index, list of target ranks)
    */
  def findRankStatistics(dataFrame: DataFrame, ranks: List[Long]):
    Map[Int, Iterable[Double]] = {

    val valueColumnPairs: RDD[(Double, Int)] = getValueColumnPairs(dataFrame)

    val sortedValueColumnPairs = valueColumnPairs.sortByKey()
    sortedValueColumnPairs.persist(StorageLevel.MEMORY_AND_DISK)

    val numOfColumns = dataFrame.schema.length
    val partitionColumnsFreq =
      getColumnsFreqPerPartition(sortedValueColumnPairs, numOfColumns)
    val ranksLocations  =
      getRanksLocationsWithinEachPart(ranks, partitionColumnsFreq, numOfColumns)

    val targetRanksValues =
      findTargetRanksIteratively(sortedValueColumnPairs, ranksLocations)
    targetRanksValues.groupByKey().collectAsMap()
  }

  /**
   * Step 1. Map the rows to pairs of ((value, colIndex), count) where count is the
   * number of times that value and that pair appear on this partition.
   *
   * For example:
   *
   * dataFrame:
   *     1.5, 1.25, 2.0
   *     1.5,  2.5, 2.0
   *
   * The output RDD will be:
   *    ((1.5, 0), 2) ((1.25, 1), 1) ((2.5, 1), 1) ((2.0, 2), 2)
   *
   * @param dataFrame of double columns to compute the rank statistics for
   *
   * @return returns RDD of ((value, column index), count)
   */
  def getValueColumnPairs(dataFrame: DataFrame): RDD[(Double, Int)] = {
    dataFrame.rdd.flatMap{
      row: Row =>
        row.toSeq.zipWithIndex.map{
          case (v, index) => (v.toString.toDouble, index)
        }
    }
  }

  /**
   * Step 2. Find the number of elements for each column in each partition.
   *
   * For Example:
   *
   * sortedValueColumnPairs:
   *    Partition 1: ((1.5, 0), 2) ((2.0, 0), 1)
   *    Partition 2: ((4.0, 0), 3) ((3.0, 1), 1)
   *
   * numOfColumns: 3
   *
   * The output will be:
   *    [(0, [3, 0]), (1, [3, 1])]，表示，分区0中包含
   *
   * @param sortedValueColumnPairs sortedAggregatedValueColumnPairs RDD of
   *                                         ((value, column index), count)
   * @param numOfColumns the number of columns
   *
   * @return Array that contains
   *         (partition index, number of elements from every column on this partition)，
    *         其中，数组索引位置代表列索引
   */
  private def getColumnsFreqPerPartition(sortedValueColumnPairs: RDD[(Double, Int)], numOfColumns : Int): Array[(Int, Array[Long])] = {

    val zero = Array.fill[Long](numOfColumns)(0)

    def aggregateColumnFrequencies(
      partitionIndex : Int, valueColumnPairs : Iterator[(Double, Int)]): Iterator[(Int, Array[Long])] = {
      val columnsFreq : Array[Long] = valueColumnPairs.aggregate(zero)(
        (a: Array[Long], v: (Double,Int)) => {
          val (value, colIndex) = v
          a(colIndex) = a(colIndex) + 1L
          a
        },
        (a: Array[Long], b: Array[Long]) => {
          a.zip(b).map{ case(aVal, bVal) => aVal + bVal}
        }
      )

      Iterator((partitionIndex, columnsFreq))
    }

    sortedValueColumnPairs.mapPartitionsWithIndex(
      aggregateColumnFrequencies).collect()
  }

  /**
   * Step 3: For each Partition determine the index of the elements
   * that are desired rank statistics
   *
   * For Example:
   *    targetRanks: 5
   *    partitionColumnsFreq: [(0, [2, 3]), (1, [4, 1]), (2, [5, 2])]
   *    numOfColumns: 2
   *
   * The output will be:
   *    [(0, []), (1, [(0, 3)]), (2, [(1, 1)])]
   *
   * @param partitionColumnsFreq Array of
   *                             (partition index,
   *                              columns frequencies per this partition)
   *
   * @return  Array that contains
   *          (partition index, relevantIndexList)
   *           Where relevantIndexList(i) = the index
   *          of an element on this partition that matches one of the target ranks)
   */
  //tag::hashMap_step3[]
  private def getRanksLocationsWithinEachPart(targetRanks : List[Long],
         partitionColumnsFreq : Array[(Int, Array[Long])],
         numOfColumns : Int) : Array[(Int, List[(Int, Long)])]  = {

    val runningTotal = Array.fill[Long](numOfColumns)(0)

    partitionColumnsFreq.sortBy(_._1).map { case (partitionIndex, columnsFreq)=>
      val relevantIndexList = new mutable.MutableList[(Int, Long)]()

      columnsFreq.zipWithIndex.foreach{ case (colCount, colIndex)  =>
        val runningTotalCol = runningTotal(colIndex)

        val ranksHere: List[Long] = targetRanks.filter(rank =>
          runningTotalCol < rank && runningTotalCol + colCount >= rank)
        relevantIndexList ++= ranksHere.map(
          rank => (colIndex, rank - runningTotalCol))

        runningTotal(colIndex) += colCount
      }

      (partitionIndex, relevantIndexList.toList)
    }
  }
  //end::hashMap_step3[]

  /**
    * Finds rank statistics elements using ranksLocations.
    *
    * @param sortedValueColumnPairs - sorted RDD of (value, colIndex) pairs
    * @param ranksLocations Array of (partition Index, list of
    *                       (column index,
   *                         rank index of this column at this partition))
    *
    * @return returns RDD of the target ranks (column index, value)
    */
  //tag::mapPartitionsExample[]
  private def findTargetRanksIteratively(
          sortedValueColumnPairs : RDD[(Double, Int)],
          ranksLocations : Array[(Int, List[(Int, Long)])]): RDD[(Int, Double)] = {

    sortedValueColumnPairs.mapPartitionsWithIndex(
      (partitionIndex : Int, valueColumnPairs : Iterator[(Double, Int)]) => {

      val targetsInThisPart: List[(Int, Long)] = ranksLocations(partitionIndex)._2
      if (targetsInThisPart.nonEmpty) {
        val columnsRelativeIndex = targetsInThisPart.groupBy(_._1).mapValues(_.map(_._2))
        val columnsInThisPart = targetsInThisPart.map(_._1).distinct

        val runningTotals : mutable.HashMap[Int, Long]= new mutable.HashMap()
        runningTotals ++= columnsInThisPart.map(columnIndex => (columnIndex, 0L)).toMap

        valueColumnPairs.filter{
          case(value, colIndex) =>
            lazy val thisPairIsTheRankStatistic: Boolean = {
              val total = runningTotals(colIndex) + 1L
              runningTotals.update(colIndex, total)
              columnsRelativeIndex(colIndex).contains(total)
            }
            (runningTotals contains colIndex) && thisPairIsTheRankStatistic
        }.map(_.swap)
     } else {
       Iterator.empty
     }
    })
  }
}