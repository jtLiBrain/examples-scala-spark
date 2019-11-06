package com.jtLiBrain.examples.scala.regex

import org.scalatest.FunSuite

import scala.util.matching.Regex

/**
  * https://docs.scala-lang.org/tour/regular-expression-patterns.html
  * https://www.scala-lang.org/api/2.11.12/#scala.util.matching.Regex
  */
class RegExExample1 extends FunSuite {
  test("findFirstMatchIn") {
    val numberPattern: Regex = "[0-9]".r

    numberPattern.findFirstMatchIn("awesomepassword") match {
      case Some(_) => println("Password OK")
      case None => println("Password must contain a number")
    }
  }

  test("12") {
    val keyValPattern: Regex = "([0-9a-zA-Z- ]+): ([0-9a-zA-Z-#()/. ]+)".r

    val input: String =
      """background-color: #A03300;
        |background-image: url(img/header100.png);
        |background-position: top center;
        |background-repeat: repeat-x;
        |background-size: 2160px 108px;
        |margin: 0;
        |height: 108px;
        |width: 100%;""".stripMargin

    for (patternMatch <- keyValPattern.findAllMatchIn(input))
      println(s"key: ${patternMatch.group(1)} value: ${patternMatch.group(2)}")
  }
}
