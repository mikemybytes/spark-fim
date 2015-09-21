package net.mkowalski.sparkfim.runner

import net.mkowalski.sparkfim.model.MinSupport
import net.mkowalski.sparkfim.test.{SparkTest, TestDataSet}

class BigFimRunnerTest extends SparkTest {

  val bigFimSmallDataSet = TestDataSet(
    inputFile = "bigfim_data_small_input.txt",
    expectedFile = "bigfim_data_small_expected.txt",
    minSupport = MinSupport(2)
  )

  def runTest(testDataset: TestDataSet, bfsStages: Int): Unit = {
    BigFimRunner(sc, defaultPersistenceManager(), resourcePath(testDataset.inputFile), bfsStages,
      testDataset.minSupport, resultDirectoryPath).run()
    verifyResult(bigFimSmallDataSet)
  }

  test("Run with single bfs stage and minSup = 2") {
    runTest(bigFimSmallDataSet, bfsStages = 1)
  }

  test("Run 2 bfs stages") {
    runTest(bigFimSmallDataSet, bfsStages = 2)
  }

  test("Run 3 bfs stages") {
    runTest(bigFimSmallDataSet, bfsStages = 3)
  }

  test("Run 4 bfs stages") {
    runTest(bigFimSmallDataSet, bfsStages = 4)
  }

  test("Run 5 bfs stages (Apriori-only mining)") {
    runTest(bigFimSmallDataSet, bfsStages = 5)
  }

}
