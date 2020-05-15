package com.jtLiBrain.examples.spark.ml.classification

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.scalatest.FunSuite

// 梯度提升树
class GradientBoostedTreeClassifierExample extends FunSuite with DataFrameSuiteBase {
  test("") {
    // 1. 加载数据
    val data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    // 2. 拟合整个数据集，将数据集的所有标签都包含到索引中
    // 注意：该数据集中label标签只有：0.0和1.0
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)

    // 3. 自动识别类型型特征，并对其进行索引
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // 拥有大于4个不同值的特征被认为是连续型特征，小于等于4个不同值的特征被认为是类别型特征
      .fit(data)

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // 4. 训练GBT模型
    val gbt = new GBTClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(10)
      .setFeatureSubsetStrategy("auto")

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and GBT in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))

    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("predictedLabel", "label", "features").show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${1.0 - accuracy}")

    val gbtModel = model.stages(2).asInstanceOf[GBTClassificationModel]
    println(s"Learned classification GBT model:\n ${gbtModel.toDebugString}")
  }
}
