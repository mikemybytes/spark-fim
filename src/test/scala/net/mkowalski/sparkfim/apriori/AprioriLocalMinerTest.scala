package net.mkowalski.sparkfim.apriori

import net.mkowalski.sparkfim.model.Item
import net.mkowalski.sparkfim.test.BaseTest
import net.mkowalski.sparkfim.util.HorizontalDbUtil

class AprioriLocalMinerTest extends BaseTest {

  private val inputFilePartition = List(
    "1\t1 2 3 4 5",
    "2\t1 3 4",
    "3\t2 4"
  )

  test("Find singletons with support") {
    val expectedSupport = Map(Item(1) -> 2, Item(2) -> 2, Item(3) -> 2, Item(4) -> 3, Item(5) -> 1)
    val result = AprioriLocalMiner(inputIterator).mine().toMap
    verifySupportResult(expectedSupport, result)
  }

  test("Find 2-FIs with support") {
    val expectedSupport = Map(Item(2, 4) -> 2, Item(2, 5) -> 1, Item(4, 5) -> 1)
    val previousSingletons = List(Item(2), Item(4), Item(5))
    val result = AprioriLocalMiner(inputIterator, previousSingletons).mine().toMap
    verifySupportResult(expectedSupport, result)
  }

  test("Find 3-FIs with support") {
    val expectedSupport = Map(Item(2, 4, 5) -> 1)
    val previousItems = List(Item(2, 4), Item(2, 5), Item(4, 5))
    val result = AprioriLocalMiner(inputIterator, previousItems).mine().toMap
    verifySupportResult(expectedSupport, result)
  }

  test("Find none of 4-FIs") {
    val previousItems = List(Item(2, 4, 5))
    val result = AprioriLocalMiner(inputIterator, previousItems).mine().toMap
    assert(result.isEmpty)
  }

  private def inputIterator: Iterator[Array[Int]] = {
    inputFilePartition.map(HorizontalDbUtil.lineToItems).iterator
  }

  private def verifySupportResult(expectedSupport: Map[Item, Int], result: Map[Item, Int]): Unit = {
    assert(result.size == expectedSupport.size, "Result map has different size")
    expectedSupport.foreach {
      case (item, support) => assert(support == result(item), "Invalid support for " + item)
    }
  }

}
