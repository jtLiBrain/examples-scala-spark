package com.jtLiBrain.examples.scala

import org.scalatest.FunSuite

class EitherExample extends FunSuite {
  test("left and right") {
    val e1: Either[Int, String] = Left(1)
    val e2: Either[Int, String] = Right("right")

    assert(e1.isLeft)
    assert(e1.left.get == 1)

    assert(e2.isRight)
    assert(e2.right.get == "right")
  }
}
