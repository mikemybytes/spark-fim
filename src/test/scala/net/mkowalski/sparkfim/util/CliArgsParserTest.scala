package net.mkowalski.sparkfim.util

import net.mkowalski.sparkfim.test.BaseTest

class CliArgsParserTest extends BaseTest {

  test("Should handle empty args") {
    run(Map())
  }

  test("Should parse single arg with value") {
    run(
      Map("single-param" -> "77"),
      "--single-param", "77"
    )
  }

  test("Should parse multiple args") {
    run(
      Map(
        "first" -> "1",
        "second-param" -> "second",
        "yet-another_param" -> "xyz"
      ),
      "--first", "1", "--second-param", "second", "--yet-another_param", "xyz"
    )
  }

  test("Should skip invalid arg with value") {
    run(
      Map("correct" -> "OK"),
      "-invALiD", "5", "--correct", "OK"
    )
  }

  test("Should skip invalid arg without value") {
    run(
      Map("b" -> "c"),
      "a", "--b", "c"
    )
  }

  test("Should skip multiple invalid args") {
    run(
      Map("e" -> "f"),
      "a", "-b", "d", "--e", "f", "G", "h"
    )
  }

  test("Should throw exception when required param is missing") {
    intercept[IllegalArgumentException] {
      CliArgsParser.parse(Array("--a", "b", "--d", "e"), requiredParams = List("c", "f"))
    }
  }

  test("Should succeed when all required params exists") {
    run(
      Map(
        "a" -> "b",
        "c" -> "d"
      ),
      requiredParams = List("a", "b", "c", "d"),
      "--a", "b", "--c", "d"
    )
  }

  def run(expected: Map[String, String], args: String*): Unit = run(expected, List(), args: _*)

  def run(expected: Map[String, String], requiredParams: List[String], args: String*): Unit = {
    val result = CliArgsParser.parse(args.toArray)
    assert(result == expected)
  }


}
