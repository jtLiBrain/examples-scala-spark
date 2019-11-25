package com.jtLiBrain.examples.scala.collection

import org.scalatest.FunSuite

class SetExample extends FunSuite {
  test("set - string") {
    val stringSet = collection.mutable.Set[String]()

    stringSet += "s1"
    stringSet += "s1"
    stringSet += "s2"

    assert(stringSet.size == 2)
  }

  test("set - case class") {
    case class Card(name: String, age: Int)
    val cardSet = collection.mutable.Set[Card]()

    cardSet += Card("N1", 1)
    cardSet += Card("N1", 1)
    cardSet += Card("N1", 11)
    cardSet += Card("N2", 2)

    assert(cardSet.size == 3)
  }

  test("set - case class override both equals() and hashCode()") {
    case class Card(name: String, age: Int) {
      override def equals(obj: Any): Boolean = {
        if(obj.isInstanceOf[Card]) {
          val name2 = obj.asInstanceOf[Card].name
          name == name2
        } else {
          false
        }
      }

      override def hashCode(): Int = name.hashCode()
    }

    val cardSet = collection.mutable.Set[Card]()

    cardSet += Card("N1", 1)
    cardSet += Card("N1", 1)
    cardSet += Card("N1", 11)
    cardSet += Card("N2", 2)

    assert(cardSet.size == 2)
  }
}
