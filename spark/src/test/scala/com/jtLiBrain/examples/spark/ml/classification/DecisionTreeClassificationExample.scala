package com.jtLiBrain.examples.spark.ml.classification

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.scalatest.FunSuite

class DecisionTreeClassificationExample extends FunSuite with DataFrameSuiteBase {
  test("") {
    // 1. 加载数据
    val data = spark.read.format("libsvm").load("/Users/Dream/Dev/git/examples-scala-spark/data/mllib/sample_libsvm_data.txt")

    data.select("label").dropDuplicates("label").show(false)

    // 2. 拟合整个数据集，将数据集的所有标签都包含到索引中
    // 注意：该数据集中label标签只有：0.0和1.0
    val labelIndexerModel = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)

    // 3. 自动识别类型型特征，并对其进行索引
    val featureIndexerModel = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // 拥有大于4个不同值的特征被认为是连续型特征，小于等于4个不同值的特征被认为是类别型特征
      .fit(data)

    // 4. 训练决策树模型
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")

    // 5. 将标签索引转换回原始的标签
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexerModel.labels)

    // 6. 将组件串联到管道
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexerModel, featureIndexerModel, dt, labelConverter))

    // 7. 将数据集拆分为：训练集和测试集
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // 8. 训练模型
    val classifierModel = pipeline.fit(trainingData)

    // 9. 进行预测
    val predictResults = classifierModel.transform(testData)

    // 10. 查看预测结果的样例数据
    predictResults.select("predictedLabel", "label", "features").show(5)

    // 11. 对预测结果进行评估
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictResults)
    println(s"Test Error = ${(1.0 - accuracy)}")

    // 12. 打印决策树
    val treeModel = classifierModel.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println(s"学习到的决策树模型为：\n ${treeModel.toDebugString}")
  }
}
