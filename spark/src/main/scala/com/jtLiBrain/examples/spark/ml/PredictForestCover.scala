package com.jtLiBrain.examples.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

object PredictForestCover {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .config(new SparkConf())
      .appName("T")
      .master("local[*]")
      .getOrCreate()

    try {
      ml(ss)
    } finally {
      ss.stop()
    }
  }

  def ml(sparkSession:SparkSession): Unit = {
    val Array(trainData, testData) = prepareData(sparkSession).randomSplit(Array(0.9, 0.1))
    trainData.cache()
    testData.cache()

    val trainFeatureVectorData = transformToFeatureVector(trainData)

    val model = train(trainFeatureVectorData)

    val predictions = predict(model, trainFeatureVectorData)

  }

  def evaluateModel(model: DecisionTreeClassificationModel, predictResult: DataFrame): Unit = {
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("Cover_Type")
      .setPredictionCol("prediction")

    evaluator.setMetricName("accuracy").evaluate(predictResult)
    evaluator.setMetricName("f1").evaluate(predictResult)
  }

  def predict(model: DecisionTreeClassificationModel, featureVectorData: DataFrame): DataFrame = {
    model.transform(featureVectorData)
  }

  def transformToFeatureVector(trainData: DataFrame): DataFrame = {
    val inputCols = trainData.columns.filter(_ != "Cover_Type")

    val assembler = new VectorAssembler()
      .setInputCols(inputCols)
      .setOutputCol("featureVector")

    assembler.transform(trainData)
  }

  def train(trainVectorData: DataFrame): DecisionTreeClassificationModel = {
    val classifier = new DecisionTreeClassifier()
      .setSeed(Random.nextLong())
      .setLabelCol("Cover_Type")
      .setFeaturesCol("featureVector")
      .setPredictionCol("prediction")

    classifier.fit(trainVectorData)
  }

  def prepareData(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val dataWithoutHeader = sparkSession.read.
      option("inferSchema", true).
      option("header", false).
      csv("file:///Users/dream/Dev/git/examples-scala-spark/data/covtype/covtype.data")

    val colNames = Seq(
      "Elevation", "Aspect", "Slope",
      "Horizontal_Distance_To_Hydrology", "Vertical_Distance_To_Hydrology",
      "Horizontal_Distance_To_Roadways",
      "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm",
      "Horizontal_Distance_To_Fire_Points"
    ) ++ (
      (0 until 4).map(i => s"Wilderness_Area_$i")
    ) ++ (
      (0 until 40).map(i => s"Soil_Type_$i")
    ) ++ Seq("Cover_Type")


    dataWithoutHeader.toDF(colNames:_*)
      .withColumn("Cover_Type", $"Cover_Type".cast("double"))
  }
}
