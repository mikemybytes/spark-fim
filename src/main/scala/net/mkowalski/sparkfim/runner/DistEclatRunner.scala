package net.mkowalski.sparkfim.runner

import net.mkowalski.sparkfim.cache.CacheRemovePriority.High
import net.mkowalski.sparkfim.cache.PersistenceManager
import net.mkowalski.sparkfim.cache.PersistenceRddExtension._
import net.mkowalski.sparkfim.eclat.{EclatLocalPrefixGroupMiner, FrequentSingletonDatabase, InitialPrefixGroupFinder}
import net.mkowalski.sparkfim.model.{FimDataTypes, MinSupport}
import net.mkowalski.sparkfim.util.PartitioningRddExtension._
import net.mkowalski.sparkfim.util.{Logging, VerticalDbUtil}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class DistEclatRunner(val sc: SparkContext,
                      val persistenceManager: PersistenceManager,
                      val inputFilePath: String,
                      val minSup: MinSupport,
                      val firstStageDepth: Int,
                      val outputDirectoryPath: String,
                      val forcedPartitionsNum: Option[Int] = None) extends FimDataTypes with Logging {

  def run(): Unit = {
    LOG.info(s"DistEclat processing start - $this")

    val minSupBc = sc.broadcast(minSup)
    val firstStageDepthBc = sc.broadcast(firstStageDepth)

    LOG.info("Step 1: Finding frequent singletons with their TID-lists")
    val inputFile = sc.textFile(inputFilePath)
    val frequentItemIdsWithTidList: RDD[(Int, Array[Int])] = inputFile.map(VerticalDbUtil.lineToItemsWithTidList).filter {
      case (itemId, tidList) => minSupBc.value fulfilledBy tidList.length
    }.forcePartitionsNum(forcedPartitionsNum).cacheWith(persistenceManager)

    LOG.info("Step 2: Distributing frequent singletons across all nodes")
    val singletonsDbBc = sc.broadcast(
      FrequentSingletonDatabase(frequentItemIdsWithTidList.collect())
    )

    LOG.info("Step 3: Reporting frequent singletons")
    frequentItemIdsWithTidList.map {
      case (itemId, tidList) => (itemId, tidList.length)
    }.saveAsTextFile(outputPath(1))

    LOG.info("Step 4: Finding initial prefix groups")
    val initialPrefixGroups = frequentItemIdsWithTidList.mapPartitions(
      InitialPrefixGroupFinder(_, singletonsDbBc.value, minSupBc.value).find()
    ).cacheWith(persistenceManager)

    LOG.info("Step 5: Reporting frequent 2-FIs")
    initialPrefixGroups.flatMap(_.itemsWithSupport).saveAsTextFile(outputPath(2))

    persistenceManager.markUnused(frequentItemIdsWithTidList, High)

    LOG.info(s"Step 6: Mining initial prefix groups to depth $firstStageDepth with next groups finding")
    val firstStageResult = initialPrefixGroups.flatMap(
      EclatLocalPrefixGroupMiner(_, firstStageDepthBc.value, minSupBc.value).mineWithNextGroups(withDiffsets = false)
    ).forcePartitionsNum(forcedPartitionsNum).cacheWith(persistenceManager)

    LOG.info(s"Step 7: Reporting frequent FIs to depth $firstStageDepth")
    firstStageResult.flatMap {
      case (itemsWithSupport, _) => itemsWithSupport
    }.saveAsTextFile(outputPath(3))

    LOG.info(s"Step 8: Mining prefix groups of size $firstStageDepth")
    firstStageResult.flatMap {
      case (_, nextGroups) => nextGroups
    }.flatMap(
        EclatLocalPrefixGroupMiner(_, minSupBc.value).mine(withDiffsets = true)
      ).saveAsTextFile(outputPath(4))

    LOG.info("DistEclat processing finished")
  }

  private def outputPath(stage: Int) = s"$outputDirectoryPath/stage$stage"

  override def toString = s"DistEclatRunner(persistenceManager=$persistenceManager, inputFilePath=$inputFilePath, " +
    s"minSup=$minSup, firstStageDepth=$firstStageDepth, outputDirectoryPath=$outputDirectoryPath, " +
    s"forcedPartitionsNum=$forcedPartitionsNum)"

}

object DistEclatRunner extends Serializable {

  def apply(sc: SparkContext, cacheManager: PersistenceManager, inputFilePath: String, minSup: MinSupport,
            firstStageDepth: Int, outputDirectoryPath: String, forcedPartitionsNum: Option[Int] = None) =
    new DistEclatRunner(sc, cacheManager, inputFilePath, minSup, firstStageDepth, outputDirectoryPath, forcedPartitionsNum)

}
