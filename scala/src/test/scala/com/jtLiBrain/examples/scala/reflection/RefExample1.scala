package com.jtLiBrain.examples.scala.reflection

import org.scalatest.FunSuite

/**
  * 1. https://docs.scala-lang.org/overviews/reflection/typetags-manifests.html
  * 2. https://zhuanlan.zhihu.com/p/69792401
  * 3. https://dzone.com/articles/scala-classtag-a-simple-use-case
  */
class RefExample1 extends FunSuite {
  test("via the Methods typeTag, classTag, or weakTypeTag") {
    import scala.reflect.runtime.universe._
    val tt = typeTag[Int]

    println(tt.toString())

    import scala.reflect._
    val ct = classTag[String]

    println(ct.toString())
  }

  test("Using an Implicit Parameter ") {
    import scala.reflect.runtime.universe._

    def paramInfo[T](x: T)(implicit tag: TypeTag[T]): Unit = {
      val targs = tag.tpe match { case TypeRef(_, _, args) => args }
      println(s"type of $x has type arguments $targs")
    }

    paramInfo(42)
    paramInfo(List(1, 2))
  }

  test("Using a Context bound of a Type Parameter") {
    import scala.reflect.runtime.universe._

    def paramInfo[T: TypeTag](x: T): Unit = {
      val targs = typeOf[T] match { case TypeRef(_, _, args) => args }
      println(s"type of $x has type arguments $targs")
    }

    paramInfo(42)
    paramInfo(List(1, 2))
  }
}
