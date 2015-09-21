package net.mkowalski.sparkfim.driver

import net.mkowalski.sparkfim.model.MinSupport
import net.mkowalski.sparkfim.runner.BigFimRunner
import net.mkowalski.sparkfim.util.{CliArgsParser, DriverUtil, Logging}

object BigFimDriver extends Logging {

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      printUsageInfo()
      return
    }

    LOG.debug("Parsing program arguments...")
    val params = CliArgsParser.parse(args, requiredParams = List(
      "inputFile", "bfsStages", "minSup", "outputDir"
    ))

    val inputFilePath = params.get("inputFile").get
    val bfsStages = params.get("bfsStages").get.toInt
    val minSup = params.get("minSup").get.toInt
    val outputDirPath = params.get("outputDir").get
    val persistenceManager = DriverUtil.createPersistenceManager(params)
    val forcedPartitionsNum = params.get("forcedPartitionsNum").map(_.toInt)
    LOG.debug("All arguments provided")

    LOG.info("Creating Spark context")
    val sc = SparkContextProvider.provideForApp("Spark BigFIM")

    LOG.info("Starting BigFIM algorithm")
    BigFimRunner(sc, persistenceManager, inputFilePath, bfsStages,
      MinSupport(minSup), outputDirPath, forcedPartitionsNum).run()
    LOG.info("BigFIM algorithm finished")
    sc.stop()
  }

  private def printUsageInfo() =
    println( """BigFIM algorithm implementation (in Scala) for Apache Spark by Michal Kowalski. Required parameters:
               |   --inputFile <path to input file>
               |   --bfsStages <number of stages using Apriori-like BFS approach>
               |   --minSup <min support threshold>
               |   --outputDir <path to result directory>
               |Optional parameters:
               |   --cacheRemovePolicy auto|normal|aggressive|paranoid
               |       auto - let Spark handle all unused cached RDDs and broadcast
               |       normal - force nonblocking unpersist for some unused objects
               |       aggressive (default) - force nonblocking unpersist for all unused objects
               |       paranoid - force unpersist for all unused objects
               |   --serializedStorage serialize objects to cache (default: false)
               |   --allowDiskStorage allow storing the partitions that don't fit on disk (default: false)
               |   --forcedPartitionsNum force repartitioning of the original data with specified partitions number
             """.stripMargin)

}
