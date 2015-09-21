package net.mkowalski.sparkfim.apriori

import net.mkowalski.sparkfim.model.{Item, MinSupport}
import net.mkowalski.sparkfim.test.SparkTest

class AprioriTidListMinerTest extends SparkTest {

  private val inputFilePartition = List(
    (1, Array(1, 2, 3, 4, 5)),
    (2, Array(1, 3, 4)),
    (3, Array(2, 4))
  )

  test("Find 2-FIs TID-lists") {
    val expectedTids = Map(
      Item(2, 4) -> Array(1, 3),
      Item(2, 5) -> Array(1),
      Item(4, 5) -> Array(1)
    )
    val previousSingletons = List(Item(2), Item(4), Item(5))
    val result = runTest(MinSupport(1), previousSingletons)
    verifyTidListResult(expectedTids, result)
  }

  test("Find 3-FIs TID-lists") {
    val expectedTids = Map(Item(2, 4, 5) -> Array(1))
    val previousItems = List(Item(2, 4), Item(2, 5), Item(4, 5))
    val result = runTest(MinSupport(1), previousItems)
    verifyTidListResult(expectedTids, result)
  }

  test("Find none of 4-FIs TID-lists") {
    val previousItems = List(Item(2, 4, 5))
    val result = runTest(MinSupport(1), previousItems)
    assert(result.isEmpty)
  }

  test("Should return only frequent TID-lists") {
    val expectedTids = Map(
      Item(2, 4) -> Array(1, 3)
    )
    val previousSingletons = List(Item(2), Item(4), Item(5))
    val result = runTest(MinSupport(2), previousSingletons)
    verifyTidListResult(expectedTids, result)
  }

  private def runTest(minSupport: MinSupport, previousItems: List[Item]): Map[Item, Array[Int]] = {
    val minSupBc = sc.broadcast(minSupport)
    val candidatesBc = sc.broadcast(CandidatesGenerator.generateFrom(previousItems))
    val result = AprioriTidListMiner(minSupBc).mine(sc.parallelize(inputFilePartition), candidatesBc)
    result.collect().toMap
  }

  private def verifyTidListResult(expectedTids: Map[Item, Array[Int]], result: Map[Item, Array[Int]]): Unit = {
    assert(result.size == expectedTids.size, "Result map has different size")
    expectedTids.foreach {
      case (item, tids) => assert(tids sameElements result(item), "Invalid TID-list for " + item)
    }
  }

}
