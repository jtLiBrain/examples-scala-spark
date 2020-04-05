package com.jtLiBrain.examples.spark.ml.feature.extractors

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.scalatest.FunSuite

class TFIDFExample extends FunSuite with DataFrameSuiteBase {
  test("hashingTF") {
    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    // 1. 将数据转换为特征化的词频向量（其中，向量为稀疏向量）
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors
    featurizedData.show(false)
    /*
    原始的words向量通过hash函数映射为一个词的索引，基于该索引计算得到词频。
    这个方案不会计算全局的“词项-索引”的映射关系，因为这在大数据量下开销非常大，因此就会遇到哈希碰撞的问题，这时不同的特征经过哈希后会为相同的词项。例如：
    [hi, i, heard, about, spark]经过哈希后被映射为4个特征：索引为[0,5,9,17], 词频为：[1.0,1.0,1.0,2.0]
    +-----+-----------------------------------+------------------------------------------+-----------------------------------------+
    |label|sentence                           |words                                     |rawFeatures                              |
    +-----+-----------------------------------+------------------------------------------+-----------------------------------------+
    |0.0  |Hi I heard about Spark             |[hi, i, heard, about, spark]              |(20,[0,5,9,17],[1.0,1.0,1.0,2.0])        |
    |0.0  |I wish Java could use case classes |[i, wish, java, could, use, case, classes]|(20,[2,7,9,13,15],[1.0,1.0,3.0,1.0,1.0]) |
    |1.0  |Logistic regression models are neat|[logistic, regression, models, are, neat] |(20,[4,6,13,15,18],[1.0,1.0,1.0,1.0,1.0])|
    +-----+-----------------------------------+------------------------------------------+-----------------------------------------+
    */

    // 2.
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    // 3.
    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.show(false)
    /*
    +-----+-----------------------------------+------------------------------------------+-----------------------------------------+----------------------------------------------------------------------------------------------------------------------+
    |label|sentence                           |words                                     |rawFeatures                              |features                                                                                                              |
    +-----+-----------------------------------+------------------------------------------+-----------------------------------------+----------------------------------------------------------------------------------------------------------------------+
    |0.0  |Hi I heard about Spark             |[hi, i, heard, about, spark]              |(20,[0,5,9,17],[1.0,1.0,1.0,2.0])        |(20,[0,5,9,17],[0.6931471805599453,0.6931471805599453,0.28768207245178085,1.3862943611198906])                        |
    |0.0  |I wish Java could use case classes |[i, wish, java, could, use, case, classes]|(20,[2,7,9,13,15],[1.0,1.0,3.0,1.0,1.0]) |(20,[2,7,9,13,15],[0.6931471805599453,0.6931471805599453,0.8630462173553426,0.28768207245178085,0.28768207245178085]) |
    |1.0  |Logistic regression models are neat|[logistic, regression, models, are, neat] |(20,[4,6,13,15,18],[1.0,1.0,1.0,1.0,1.0])|(20,[4,6,13,15,18],[0.6931471805599453,0.6931471805599453,0.28768207245178085,0.28768207245178085,0.6931471805599453])|
    +-----+-----------------------------------+------------------------------------------+-----------------------------------------+----------------------------------------------------------------------------------------------------------------------+
   */

    // 4.
    // 得到的rescaledData特征数据可以进一步传递给下游的学习模型，之所以要进行由rawFeatures向量到features向量的转换是为了提高下游计算性能
  }
}