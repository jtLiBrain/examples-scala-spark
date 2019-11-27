package com.jtLiBrain.examples.spark.sql.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, ObjectType, StructField, StructType}

/**
  * https://stackoverflow.com/questions/36629916/why-mutable-map-becomes-immutable-automatically-in-userdefinedaggregatefunction
  */
class AggregatorWithObject extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType (
    StructField("mapV", DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType))
      :: Nil
  )

  override def bufferSchema: StructType = StructType (
    StructField("mapV", ObjectType(classOf[collection.mutable.Map[String, Long]]))
      :: Nil
  )

  override def dataType: DataType = bufferSchema

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = collection.mutable.Map[String, Long]()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)) {
      val newIndicatorMap = input.getMap[String, Long](0)
      val bufferIndicatorMap = buffer.getAs[collection.mutable.Map[String, Long]](0)

      mergeMap(newIndicatorMap, bufferIndicatorMap)

      buffer(0) = bufferIndicatorMap
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if(!buffer2.isNullAt(0)) {
      val newIndicatorMap = buffer2.getAs[collection.mutable.Map[String, Long]](0)
      val bufferIndicatorMap = buffer1.getAs[collection.mutable.Map[String, Long]](0)

      mergeMap(newIndicatorMap, bufferIndicatorMap)
    }
  }

  def mergeMap(srcMap: scala.collection.Map[String, Long], targetMap: collection.mutable.Map[String, Long]): Unit = {
    srcMap.foreach{ case (k,v) =>
      val valueInBuffer = targetMap.get(k).getOrElse(0L)

      targetMap += k -> {v+valueInBuffer}
    }
  }

  override def evaluate(buffer: Row): Any = buffer
}