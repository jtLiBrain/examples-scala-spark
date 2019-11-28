package com.jtLiBrain.examples.spark.sql.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

class AggregatorWithImmutableMap extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType (
    StructField("mapV", DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType))
      :: Nil
  )

  override def bufferSchema: StructType = StructType (
    StructField("mapV", DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType))
      :: Nil
  )

  override def dataType: DataType = bufferSchema

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Long]()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)) {
      val newIndicatorMap = input.getMap[String, Long](0)
      val bufferIndicatorMap = buffer.getAs[Map[String, Long]](0)

      buffer(0) = mergeMap(newIndicatorMap, bufferIndicatorMap)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if(!buffer2.isNullAt(0)) {
      val newIndicatorMap = buffer2.getAs[Map[String, Long]](0)
      val bufferIndicatorMap = buffer1.getAs[Map[String, Long]](0)

      buffer1(0) = mergeMap(newIndicatorMap, bufferIndicatorMap)
    }
  }

  def mergeMap(srcMap: scala.collection.Map[String, Long], targetMap: Map[String, Long]): Map[String, Long] = {
    var tmpMap = targetMap // TODO for performance, tmpMap can be create as a mutable map

    srcMap.foreach{ case (k,v) =>
      val valueInBuffer = targetMap.get(k).getOrElse(0L)

      tmpMap += k -> {v+valueInBuffer}
    }

    tmpMap
  }

  override def evaluate(buffer: Row): Any = buffer
}