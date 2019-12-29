package com.jtLiBrain.examples.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer, VectorIndexerModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

object PredictForestCoverV2 {
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

    val model = train(trainData)

    model.bestModel


  }


  def train(trainData: DataFrame): TrainValidationSplitModel = {
    val inputCols = trainData.columns.filter(_ != "Cover_Type")

    val assembler = new VectorAssembler()
      .setInputCols(inputCols)
      .setOutputCol("featureVector")

    VectorIndexerModel

    val indexer = new VectorIndexer()
      .setMaxCategories(40)
      .setInputCol("featureVector")
      .setOutputCol("indexedVector")

    val classifier = new DecisionTreeClassifier()
      .setSeed(Random.nextLong())
      .setLabelCol("Cover_Type")
      .setFeaturesCol("indexedVector")
      .setPredictionCol("prediction")

    val pipeline = new Pipeline().setStages(Array(
      assembler, indexer, classifier
    ))

    val paramGrid = new ParamGridBuilder()
      .addGrid(classifier.impurity, Seq("gini", "entropy"))
      .addGrid(classifier.maxDepth, Seq(1, 20))
      .addGrid(classifier.maxBins, Seq(40, 300))
      .addGrid(classifier.minInfoGain, Seq(0.0, 0.05))
      .build()

    val multiclassEval = new MulticlassClassificationEvaluator()
      .setLabelCol("Cover_Type")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val validator = new TrainValidationSplit()
      .setSeed(Random.nextLong())
      .setEstimator(pipeline)
      .setEvaluator(multiclassEval)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.9)

    validator.fit(trainData)
  }

  def unencodeOneHot(data: DataFrame, sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    val wildernessCols = (0 until 4).map(i => s"Wilderness_Area_$i").toArray
    val wildernessAssembler = new VectorAssembler()
      .setInputCols(wildernessCols)
      .setOutputCol("wilderness")

    val unhotUDF = udf((vec: Vector) => vec.toArray.indexOf(1.0).toDouble)
    val withWilderness = wildernessAssembler.transform(data)
      .drop(wildernessCols:_*)
      .withColumn("wilderness", unhotUDF($"wilderness"))
    val soilCols = (0 until 40).map(i => s"Soil_Type_$i").toArray
    val soilAssembler = new VectorAssembler().
      setInputCols(soilCols).
      setOutputCol("soil")
    soilAssembler.transform(withWilderness).
      drop(soilCols:_*).
      withColumn("soil", unhotUDF($"soil"))
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
