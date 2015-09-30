package net.mkowalski.sparkfim.runner

import net.mkowalski.sparkfim.apriori.{AprioriLocalMiner, AprioriTidListMiner, CandidatesGenerator}
import net.mkowalski.sparkfim.cache.CacheRemovePriority.{High, Low}
import net.mkowalski.sparkfim.cache.PersistenceRddExtension._
import net.mkowalski.sparkfim.cache.{CacheRemovePriority, PersistenceManager}
import net.mkowalski.sparkfim.eclat.EclatLocalPrefixGroupMiner
import net.mkowalski.sparkfim.model._
import net.mkowalski.sparkfim.util.PartitioningRddExtension._
import net.mkowalski.sparkfim.util.{AggUtil, HorizontalDbUtil, Logging}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

class BigFimRunner(val sc: SparkContext,
                   val persistenceManager: PersistenceManager,
                   val inputFilePath: String,
                   val bfsStages: Int,
                   val minSup: MinSupport,
                   val outputDirectoryPath: String,
                   val forcedPartitionsNum: Option[Int] = None) extends FimDataTypes with Logging {

  require(bfsStages > 0, "At least one BFS (Apriori) stage is required")

  def run(): Unit = {
    LOG.info(s"BigFIM processing start - $this")

    val minSupBc = sc.broadcast(minSup)

    val inputFile = sc.textFile(inputFilePath)
    val inputTidWithItems = inputFile
      .map(HorizontalDbUtil.lineToTidWithItems)
      .filter {
      case (_, items) => items.nonEmpty
    }.forcePartitionsNum(forcedPartitionsNum).cacheWith(persistenceManager)
    val inputItems = inputTidWithItems.map(_._2)

    LOG.info(s"Step 1: generating k-FIs (k=$bfsStages) using BFS (Apriori-like)")
    val bfsSingletonsBc = performBfsStages(inputItems, minSupBc, sc.broadcast(List.empty[ItemWithSupport]), 1)
    val frequentItemIdsBc = sc.broadcast(bfsSingletonsBc.value.map {
      case (item, support) => item.extension
    })
    val filteredInputTidWithItems = inputTidWithItems.map {
      case (tid, items) => (tid, items.intersect(frequentItemIdsBc.value))
    }.cacheWith(persistenceManager)
    persistenceManager.markUnused(inputItems, Low)
    persistenceManager.markUnused(inputTidWithItems, Low)

    val bfsResultBc = bfsStages match {
      case 1 => bfsSingletonsBc
      case _ => performBfsStages(filteredInputTidWithItems.map(_._2), minSupBc, bfsSingletonsBc, bfsStages, currentStage = 2)
    }

    LOG.info(s"Step 2: generating k+1-FIs TID-lists (k=$bfsStages) using BFS")

    val candidatesBc = sc.broadcast(
      CandidatesGenerator.generateFrom(bfsResultBc.value.map(_._1))
    )
    persistenceManager.markUnused(bfsResultBc)

    val tidLists = AprioriTidListMiner(minSupBc)
      .mine(filteredInputTidWithItems, candidatesBc)
      .cacheWith(persistenceManager)

    tidLists.map {
      case (item, tidList) => (item, tidList.length)
    }.saveAsTextFile(outputPath(bfsStages + 1))

    persistenceManager.markUnused(inputTidWithItems, High)
    persistenceManager.markUnused(candidatesBc)

    LOG.info(s"Step 3: finding & mining k+1 prefix groups (k=$bfsStages)")
    val prefixGroups = tidLists
      .map {
      case (item, tidList) =>
        val extension = PrefixGroupExtension(item.extension, tidList.toArray)
        (item.prefix, extension)
    }
    prefixGroups.aggregateByKey(AggUtil.zero[PrefixGroupExtension])(
      AggUtil.seqOp[PrefixGroupExtension], AggUtil.combOp[PrefixGroupExtension]
    ).flatMap {
      case (prefix, extensions) =>
        val prefixGroup = PrefixGroup(prefix, extensions.toList)
        EclatLocalPrefixGroupMiner(prefixGroup, minSupBc.value).mine()
    }.saveAsTextFile(outputPath(bfsStages + 2))

    LOG.info("BigFIM processing finished")
  }

  override def toString = s"BigFimRunner(persistenceManager=$persistenceManager, inputFilePath=$inputFilePath, " +
    s"bfsStages=$bfsStages, minSup=$minSup, outputDirectoryPath=$outputDirectoryPath, " +
    s"forcedPartitionsNum=$forcedPartitionsNum)"

  @tailrec
  private def performBfsStages(inputFileItems: RDD[Array[Int]],
                               minSupBc: Broadcast[MinSupport],
                               previousItems: Broadcast[List[ItemWithSupport]],
                               maxStages: Int,
                               currentStage: Int = 1): Broadcast[List[ItemWithSupport]] = {

    LOG.debug(s"BFS stage $currentStage start")
    val resultRdd = inputFileItems.mapPartitions(
      partition => previousItems.value match {
        case Nil => AprioriLocalMiner(partition).mine()
        case _ => AprioriLocalMiner(partition, previousItems.value.map(_._1)).mine()
      }
    ).reduceByKey(_ + _).filter {
      case (item, support) => minSupBc.value fulfilledBy support
    }.cacheWith(persistenceManager)

    LOG.debug(s"Saving stage $currentStage result")
    resultRdd.saveAsTextFile(outputPath(currentStage))

    val resultList = resultRdd.collect().toList
    persistenceManager.markUnused(resultRdd, CacheRemovePriority.High)

    LOG.debug(s"Finishing BFS stage $currentStage")
    resultList match {
      case Nil =>
        LOG.trace("Empty result - skipping further stages")
        sc.broadcast(resultList)
      case _ if currentStage == maxStages =>
        LOG.trace("Last BFS stage - broadcasting result")
        sc.broadcast(resultList)
      case _ =>
        LOG.trace("Preparing next BFS stage")
        persistenceManager.markUnused(previousItems)

        val previousBc = sc.broadcast(resultList)
        performBfsStages(inputFileItems, minSupBc, previousBc, maxStages, currentStage + 1)
    }

  }

  private def outputPath(stage: Int) = s"$outputDirectoryPath/stage$stage"

}

object BigFimRunner {

  def apply(sc: SparkContext, persistenceManager: PersistenceManager, inputFilePath: String, bfsStages: Int,
            minSup: MinSupport, outputDirPath: String, forcedPartitionsNum: Option[Int] = None) =
    new BigFimRunner(sc, persistenceManager, inputFilePath, bfsStages, minSup, outputDirPath, forcedPartitionsNum)

}
