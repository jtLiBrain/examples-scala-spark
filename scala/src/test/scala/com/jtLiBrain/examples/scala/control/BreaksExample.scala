package com.jtLiBrain.examples.scala.control

import org.scalatest.FunSuite
import scala.util.control.Breaks

class BreaksExample extends FunSuite {
  test("breakable and break") {
    val nums = Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

    println("case 1:")
    Breaks.breakable {
      var i = 0
      while (true) {
        println(nums(i))
        i += 1
        if(i == 5) Breaks.break
      }
    }

    println("case 2:")
    Breaks.breakable {
      nums.foreach(n => {
        if(n == 5) Breaks.break
        else println(n)
      })
    }
  }
}
