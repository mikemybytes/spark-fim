package net.mkowalski.sparkfim.runner

import net.mkowalski.sparkfim.model.MinSupport
import net.mkowalski.sparkfim.test.{SparkTest, TestDataSet}

class DistEclatRunnerTest extends SparkTest {

  val distEclatSmallDataSet = TestDataSet(
    inputFile = "disteclat_data_small_input.txt",
    expectedFile = "disteclat_data_small_expected.txt",
    minSupport = MinSupport(2)
  )

  def runTest(firstStageDepth: Int): Unit = {
    DistEclatRunner(
      sc, defaultPersistenceManager(), resourcePath(distEclatSmallDataSet.inputFile),
      distEclatSmallDataSet.minSupport, firstStageDepth, resultDirectoryPath
    ).run()
    verifyResult(distEclatSmallDataSet)
  }

  test("Run with firstStageDepth=1") {
    runTest(1)
  }

  test("Run with firstStageDepth=2") {
    runTest(2)
  }

  test("Run with firstStageDepth=3") {
    runTest(3)
  }

  test("Run with firstStageDepth=4") {
    runTest(4)
  }

  test("Run with firstStageDepth=5") {
    runTest(5)
  }

}
