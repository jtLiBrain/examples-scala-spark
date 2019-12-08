package com.jtLiBrain.examples.spark.goldilocks

import com.jtLiBrain.examples.spark.goldilocks.GoldilocksTestData.PandaInfo
import org.apache.spark.sql.{DataFrame, SQLContext, SQLImplicits, SparkSession}

trait GoldilocksTestData {self =>
  protected def spark: SparkSession

  private object internalImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.spark.sqlContext
  }

  import internalImplicits._
  import GoldilocksTestData._

  protected lazy val pandaInfo: DataFrame = {
    val d = Array(
      PandaInfo("Mama Panda",             15.0, 0.25, 2467.0, 0.0),
      PandaInfo("Papa Panda",             2.0,  1000, 35.4,   0.0),
      PandaInfo("Baby Panda",             10.0, 2.0,  50.0,   0.0),
      PandaInfo("Baby Panda's toy Panda", 3.0,  8.5,  0.2,    98.0)
    )

    val df = spark.sparkContext.parallelize(d).toDF()
    df.createOrReplaceTempView("pandaInfo")
    df
  }
}

object GoldilocksTestData {
  case class PandaInfo(name: String, happiness: Double, niceness: Double, softness: Double, sweetness: Double)
}