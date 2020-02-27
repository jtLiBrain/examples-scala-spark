package com.jtLiBrain.examples.scala.comprehensions

import org.scalatest.FunSuite

/**
  * https://docs.scala-lang.org/tour/for-comprehensions.html
  * https://scala-lang.org/files/archive/spec/2.13/06-expressions.html#for-comprehensions-and-for-loops
  * for-推导式
  */
class ForExample1 extends FunSuite {
  test("one generator") {
    case class User(name: String, age: Int)

    val userBase = List(
      User("Travis",    28),
      User("Kelly",     33),
      User("Jennifer",  44),
      User("Dennis",    23)
    )

    val twentySomethings =
      for (user <- userBase if (user.age >=20 && user.age < 30))
        yield user.name  // i.e. add this to a list

    twentySomethings.foreach(name => println(name))  // prints Travis and Dennis
  }

  test("two generator2") {
    def foo(n: Int, v: Int) =
      for (i <- 0 until n;
           j <- 0 until v)
        yield (i, j)

    foo(2, 3) foreach {
      case (i, j) =>
        println(s"($i, $j) ")
    }
  }
}
