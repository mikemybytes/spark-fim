package net.mkowalski.sparkfim.test

import net.mkowalski.sparkfim.model.MinSupport

class TestDataSet(val inputFile: String, val expectedFile: String, val minSupport: MinSupport) {

}

object TestDataSet {

  def apply(inputFile: String, expectedFile: String, minSupport: MinSupport) =
    new TestDataSet(inputFile, expectedFile, minSupport)

}