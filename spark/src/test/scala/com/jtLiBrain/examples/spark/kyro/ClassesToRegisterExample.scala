package com.jtLiBrain.examples.spark.kyro

import org.scalatest.FunSuite

class ClassesToRegisterExample extends FunSuite {
  test("how to register class with spark.kryo.classesToRegister") {
    val t1 = new Array[Array[Byte]](1)
    val t2 = new Array[String](2)

    System.out.println(t1.getClass.getName)
    System.out.println(t2.getClass.getName)
  }
}
