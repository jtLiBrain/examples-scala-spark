package com.jtLiBrain.examples.scala.collection

import org.scalatest.FunSuite

/**
 * 1. https://docs.scala-lang.org/overviews/collections/seqs.html
 */
class SeqExample extends FunSuite {
  /*
  patch: from: Int, patch: GenSeq[B], replaced: Int
  将当前Seq，从索引from处开始后面replaced个元素用patch进行替换
   */
  test("seq - patch") {
    val s1 = Seq(0, 1, 2, 3, 4, 5, 6)
    val s2 = s1.patch(2, Seq(9,9,9,9,9), 2)
    val s3 = s1.patch(2, Seq(9,9,9,9,9), 3)
    val s4 = s1.patch(2, Seq(9,9,9,9,9), 4)
    println(s2.toString())
    println(s3.toString())
    println(s4.toString())
  }
}
