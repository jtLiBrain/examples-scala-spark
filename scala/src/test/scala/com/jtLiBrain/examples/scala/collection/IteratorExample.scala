package com.jtLiBrain.examples.scala.collection

import org.scalatest.FunSuite

/**
 * https://www.scala-lang.org/api/2.12.2/scala/collection/Iterator.html
 */
class IteratorExample extends FunSuite {
  test("sliding") {
    val iter = Iterator(1, 2, 3, 4, 5)
    val slidingIterator = iter.sliding(3)

    while (slidingIterator.hasNext) {
      val intList = slidingIterator.next()
      println(intList.mkString(","))
    }
  }
}
