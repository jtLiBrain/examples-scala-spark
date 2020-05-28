package com.jtLiBrain.examples.scala.collection

import org.scalatest.FunSuite

class IndexedSeqOptimizedExample extends FunSuite {
  test("dropWhile") {
    val array = Array(1, 2, 3, 4, 5)
    val changedArray = array.dropWhile(_ == 1)

    assert(changedArray === Array(2, 3, 4, 5))
  }

  test("takeWhile") {
    val array = Array(1, 2, 3, 4, 5)
    val changedArray = array.takeWhile(_ == 1)

    assert(changedArray === Array(1))
  }
}
