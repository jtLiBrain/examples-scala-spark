package com.jtLiBrain.examples.spark.sql.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

/**
  * https://stackoverflow.com/questions/36629916/why-mutable-map-becomes-immutable-automatically-in-userdefinedaggregatefunction
  *
  *
  * org.apache.spark.sql.catalyst.CatalystTypeConverters#MapConverter#toCatalystImpl(scalaValue: Any)
  * ```
  * override def toCatalystImpl(scalaValue: Any): MapData = {
  *   val keyFunction = (k: Any) => keyConverter.toCatalyst(k)
  *   val valueFunction = (k: Any) => valueConverter.toCatalyst(k)
  *
  *   scalaValue match {
  *     case map: Map[_, _] => ArrayBasedMapData(map, keyFunction, valueFunction)
  *     case javaMap: JavaMap[_, _] => ArrayBasedMapData(javaMap, keyFunction, valueFunction)
  *     case other => throw new IllegalArgumentException(
  *       s"The value (${other.toString}) of the type (${other.getClass.getCanonicalName}) "
  *         + "cannot be converted to a map type with "
  *         + s"key type (${keyType.catalogString}) and value type (${valueType.catalogString})")
  *   }
  * }
  * ```
  *
  * It seems that any map types must be converted to corresponding map type in catalyst framework.
  */
class AggregatorWithMutableMap extends UserDefinedAggregateFunction {
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