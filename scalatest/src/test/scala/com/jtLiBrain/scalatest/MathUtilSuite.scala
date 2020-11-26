package com.jtLiBrain.scalatest

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class MathUtilSuite extends AnyFlatSpec with should.Matchers {
  "MathUtil" should "pop values in last-in-first-out order" in {
    MathUtil.plus(1, 2) shouldBe 3
  }
}
