package com.jtLiBrain.examples.scala.control

import org.scalatest.FunSuite

import scala.util.{Failure, Success, Try}

class TryExample extends FunSuite {
  test("try and get") {
    Try(1+1) match {
      case Success(n) => println(n)
      case Failure(e) => println(e)
    }
    println(Try(1+1).getOrElse(3))

    Try(1/0) match {
      case Success(n) => println(n)
      case Failure(e) => println(e)
    }
    println(Try(1/0).getOrElse(0))
  }
}
