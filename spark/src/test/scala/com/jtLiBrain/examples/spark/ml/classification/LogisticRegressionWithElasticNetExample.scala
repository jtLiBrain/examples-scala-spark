package com.jtLiBrain.examples.spark.ml.classification

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.classification.LogisticRegression
import org.scalatest.FunSuite

class LogisticRegressionWithElasticNetExample extends FunSuite with DataFrameSuiteBase {
  test("") {
    // Load training data
    val training = spark.read.format("libsvm").load("/Users/Dream/Dev/git/examples-scala-spark/data/mllib/sample_libsvm_data.txt")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3) // 正则化参数，相当于λ
      .setElasticNetParam(0.8) // α

    // Fit the model
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // We can also use the multinomial family for binary classification
    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFamily("multinomial")

    val mlrModel = mlr.fit(training)

    // Print the coefficients and intercepts for logistic regression with multinomial family
    println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
    println(s"Multinomial intercepts: ${mlrModel.interceptVector}")
  }
}
