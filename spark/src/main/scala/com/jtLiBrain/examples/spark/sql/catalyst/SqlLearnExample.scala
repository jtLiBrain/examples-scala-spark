package com.jtLiBrain.examples.spark.sql.catalyst

import org.apache.spark.sql.SparkSession

object SqlExample {
  class TreeNode(name: String) {
    def nodeName: String = name
    var children: Seq[TreeNode] = Seq.empty[TreeNode]
    def foreach(f: TreeNode => Unit): Unit = {
      f(this)
      children.foreach(_.foreach(f))
    }
    def map[A](f: TreeNode => A): Seq[A] = {
      val ret = new collection.mutable.ArrayBuffer[A]()
      foreach(ret += f(_))
      ret
    }
  }

  def main1(args: Array[String]): Unit = {
    val n1 = new TreeNode("n1")
    val n2 = new TreeNode("n2")
    val n11 = new TreeNode("n11")
    val n12 = new TreeNode("n12")

    val root = new TreeNode("root")

    val s1 = Seq(n1, n2)
    val s2 = Seq(n11, n12)

    root.children = s1
    n1.children = s2

    root.map(_.nodeName).foreach(println(_))
  }

  def main3(args: Array[String]): Unit = {
    val a = Array(1, 2, 3, 4, 5)
    val ar = a.scanLeft(0)(_+_)
    ar.foreach(println(_))
  }

  def main2(args: Array[String]): Unit = {

    val d = Seq(1, 2, 3)
    val r = d.foldLeft(""){(s, i) =>
      val ss = s + "+" + i
      val sss = s"s is $s, i is $i -> $ss"
      println(sss)
      ss
    }

    println(r)
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Test SQL")
      .getOrCreate()

    sparkSession.read
      .option("inferSchema", false)
      .option("header", true)
      .csv("student.csv")
      .createOrReplaceTempView("student")

    sparkSession.read
      .option("inferSchema", false)
      .option("header", true)
      .csv("score.csv")
      .createOrReplaceTempView("score")

    val sql =
      """
        |SELECT avg(total) AS avgScore
        |FROM (
        |  SELECT
        |    student.id,
        |    ( 100 + 80 + score.mathScore + score.englishScore ) AS total
        |  FROM student
        |  JOIN score ON (student.id = score.studentId)
        |  WHERE student.age < 35
        |) tmp
        |""".stripMargin
    val sqlDF = sparkSession.sql(sql)
    sqlDF.take(1).foreach(r => println(r.get(0).toString))
    println(sqlDF.queryExecution)

    sparkSession.stop()
  }
}

