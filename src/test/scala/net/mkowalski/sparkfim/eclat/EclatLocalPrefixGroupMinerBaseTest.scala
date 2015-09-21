package net.mkowalski.sparkfim.eclat

import net.mkowalski.sparkfim.model._
import net.mkowalski.sparkfim.test.BaseTest

abstract class EclatLocalPrefixGroupMinerBaseTest extends BaseTest with FimDataTypes {

  val minSupport = MinSupport(2)

  val groupWithEmptyPrefix = PrefixGroup(List(
    PrefixGroupExtension(1, Array(2, 5)),
    PrefixGroupExtension(2, Array(1, 2, 3, 4)),
    PrefixGroupExtension(3, Array(1, 3, 4)),
    PrefixGroupExtension(4, Array(2, 3, 4)),
    PrefixGroupExtension(5, Array(2, 3, 4))
  ))

  val groupWithEmptyPrefixExpectedSupport = Map(
    Item(2, 3) -> 3,
    Item(2, 4) -> 3,
    Item(2, 5) -> 3,
    Item(3, 4) -> 2,
    Item(3, 5) -> 2,
    Item(4, 5) -> 3,
    Item(2, 3, 4) -> 2,
    Item(2, 3, 5) -> 2,
    Item(2, 4, 5) -> 3,
    Item(3, 4, 5) -> 2,
    Item(2, 3, 4, 5) -> 2
  )

  val groupWithSingleElement = PrefixGroup(Prefix(1), List(
    PrefixGroupExtension(2, Array(1, 2, 3, 4)),
    PrefixGroupExtension(3, Array(1, 3, 4)),
    PrefixGroupExtension(4, Array(2, 3, 4)),
    PrefixGroupExtension(5, Array(2, 5))
  ))

  val groupWithSingleElementExpectedSupport = Map(
    Item(1, 2, 3) -> 3,
    Item(1, 2, 4) -> 3,
    Item(1, 3, 4) -> 2,
    Item(1, 2, 3, 4) -> 2
  )

  def testExpectingEmptyResult(prefixGroup: PrefixGroup, depthLimit: Int) = {
    val result = mineWithLimit(prefixGroup, depthLimit)
    assert(result.isEmpty, s"This prefix group should not produce FIs with given depth limit ($depthLimit)")
  }

  test("Mine prefix group with single extension without depth limit") {
    val input = PrefixGroup(List(
      PrefixGroupExtension(1, Array(1, 2, 3, 4))
    ))
    val result = mine(input)
    assert(result.isEmpty, "This prefix group should not produce FIs")
  }

  def mineWithLimit(prefixGroup: PrefixGroup, depthLimit: Int): Map[Item, Int] = {
    EclatLocalPrefixGroupMiner(prefixGroup, depthLimit, minSupport).mine(mineWithDiffsets).toMap
  }

  def testExpectingNonEmptyResult(prefixGroup: PrefixGroup, expectedSuppport: Map[Item, Int], depthLimit: Int) = {
    val expectedSupport = limitToDepth(expectedSuppport, depthLimit)
    val result = mineWithLimit(prefixGroup, depthLimit)
    verifySupportResult(expectedSupport, result)
  }

  test("Mine group with empty prefix (initial group)") {
    val input = groupWithEmptyPrefix
    val expectedSupport = groupWithEmptyPrefixExpectedSupport

    testWithoutDepthLimit(input, expectedSupport)
    testExpectingEmptyResult(input, depthLimit = 1)
    testExpectingNonEmptyResult(input, expectedSupport, depthLimit = 2)
    testExpectingNonEmptyResult(input, expectedSupport, depthLimit = 3)
  }

  def limitToDepth(expectedSupport: Map[Item, Int], depthLimit: Int): Map[Item, Int] = {
    expectedSupport.filter {
      case (item, _) => item.idLength <= depthLimit
    }
  }

  private def verifySupportResult(expectedSupport: Map[Item, Int], result: Map[Item, Int]): Unit = {
    assert(result.size == expectedSupport.size, "Result map has different size")
    expectedSupport.foreach {
      case (item, support) => assert(support == result(item), "Invalid support for " + item)
    }
  }

  def testWithoutDepthLimit(prefixGroup: PrefixGroup, expectedSupport: Map[Item, Int]) = {
    val result = mine(prefixGroup)
    verifySupportResult(expectedSupport, result)
  }

  test("Mine group with single element prefix") {
    val input = groupWithSingleElement
    val expectedSupport = groupWithSingleElementExpectedSupport

    testWithoutDepthLimit(input, expectedSupport)
    testExpectingEmptyResult(input, depthLimit = 1)
    testExpectingEmptyResult(input, depthLimit = 2)
    testExpectingNonEmptyResult(input, expectedSupport, depthLimit = 3)
    testExpectingNonEmptyResult(input, expectedSupport, depthLimit = 4)
  }

  def mine(prefixGroup: PrefixGroup): Map[Item, Int] = {
    EclatLocalPrefixGroupMiner(prefixGroup, minSupport).mine(mineWithDiffsets).toMap
  }

  test("Mine group with limited depth and find next prefix group") {
    val depthLimit = 3
    val expectedSupport = limitToDepth(groupWithSingleElementExpectedSupport, depthLimit)
    val result = EclatLocalPrefixGroupMiner(groupWithSingleElement, depthLimit, minSupport)
      .mineWithNextGroups(mineWithDiffsets)
      .toList
    verifySupportResult(expectedSupport, result.head._1.toMap)

    val expectedNextPrefixGroup = PrefixGroup(Prefix(1, 2), List(
      PrefixGroupExtension(3, Array(1, 3, 4)),
      PrefixGroupExtension(4, Array(2, 3, 4))
    ))

    val nextPrefixGroups = result.head._2

    assert(nextPrefixGroups.size == 1)
    assert(nextPrefixGroups.head == expectedNextPrefixGroup)
  }

  def mineWithDiffsets: Boolean

}
