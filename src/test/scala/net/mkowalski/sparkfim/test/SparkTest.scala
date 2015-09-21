package net.mkowalski.sparkfim.test

import java.nio.file.{Files, Path}

import net.mkowalski.sparkfim.cache.{CacheRemovePolicy, PersistenceManager}
import net.mkowalski.sparkfim.driver.SparkContextProvider
import net.mkowalski.sparkfim.model.Item
import net.mkowalski.sparkfim.util.Logging
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext

abstract class SparkTest extends BaseTest with Logging {

  private var _sc: SparkContext = _

  private var _tempDir: Path = _

  def sc = _sc

  def verifyResult(dataSet: TestDataSet, result: Map[Item, Int] = resultFromFile): Unit = {
    val expectedResult = TestFileUtil.readExpectedResultFromFile(resourcePath(dataSet.expectedFile))
    assert(result.size == expectedResult.size, "Invalid result size")
    expectedResult.foreach {
      case (item, expectedSupport) =>
        val sortedItem = item.sorted()
        assert(result.contains(sortedItem), "Result does not contain: " + sortedItem)
        assert(result(sortedItem) == expectedSupport, "Invalid support value for: " + sortedItem)
    }
  }

  before {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")

    _sc = SparkContextProvider.provideForTest("SparkLocalTest_" + this.getClass.getSimpleName)

    val tempDirPrefix = s"${this.getClass.getPackage.getName}-"
    LOG.info(s"Creating temp directory with prefix: $tempDirPrefix")
    _tempDir = Files.createTempDirectory(tempDirPrefix)
    LOG.info(s"Temp directory created: ${_tempDir.getFileName}")
  }

  after {
    _sc.stop()
    _sc = null

    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")

    LOG.info(s"Removing temp directory: ${_tempDir.getFileName}")
    FileUtils.deleteDirectory(_tempDir.toFile)
  }

  def resourcePath(resourceFileName: String): String = {
    val inputFileResource = getClass.getResource("/" + resourceFileName)
    assume(inputFileResource != null, "Resource file '" + resourceFileName + "' not found")
    inputFileResource.getPath
  }

  def resultFromFile: Map[Item, Int] = TestFileUtil.resultFromFile(resultDirectoryPath)

  def resultDirectoryPath = fileSystemPath(this.getClass.getSimpleName)

  def fileSystemPath(directoryPath: String): String = {
    require(!directoryPath.contains(".."), "Test directory path must not refer to parent")
    require(!directoryPath.startsWith("/"), "Test directory path must not start with path separator")

    s"${_tempDir}/$directoryPath"
  }

  def defaultPersistenceManager() = PersistenceManager(CacheRemovePolicy.defaultPolicy)

}
