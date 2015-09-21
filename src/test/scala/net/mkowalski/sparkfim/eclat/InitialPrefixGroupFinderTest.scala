package net.mkowalski.sparkfim.eclat

import net.mkowalski.sparkfim.model._
import net.mkowalski.sparkfim.test.BaseTest

class InitialPrefixGroupFinderTest extends BaseTest with FimDataTypes {

  val singletonsDb = FrequentSingletonDatabase(Array(
    (3, Array(1, 2, 3, 4)),
    (4, Array(1, 3, 4)),
    (1, Array(1, 4)),
    (2, Array(1, 2)),
    (5, Array(1, 5))
  ))

  val minSup = MinSupport(2)

  test("Should find nothing for singleton partition") {
    val inputPartition = partition(
      (5, Array(1, 5))
    )
    runTest(inputPartition, expectedResult = List())
  }

  test("Find single prefix group in singleton partition") {
    val inputPartition = partition(
      (4, Array(1, 3, 4))
    )
    val expectedResult = List(
      PrefixGroup(
        Prefix(4), List(
          PrefixGroupExtension(1, Array(1, 4))
        ))
    )
    runTest(inputPartition, expectedResult)
  }

  test("Find prefix group in partition") {
    val inputPartition = partition(
      (3, Array(1, 2, 3, 4)),
      (1, Array(1, 4)),
      (5, Array(1, 5))
    )
    val expectedResult = List(
      PrefixGroup(
        Prefix(3), List(
          PrefixGroupExtension(4, Array(1, 3, 4)),
          PrefixGroupExtension(1, Array(1, 4)),
          PrefixGroupExtension(2, Array(1, 2))
        )
      )
    )
    runTest(inputPartition, expectedResult)
  }

  def runTest(inputPartition: Iterator[ItemIdWithTidList], expectedResult: List[PrefixGroup]) = {
    val result = InitialPrefixGroupFinder(inputPartition, singletonsDb, minSup).find()
    assert(result.toSet == expectedResult.toSet)
  }

  def partition[T](elements: T*): Iterator[T] = {
    List(elements: _*).iterator
  }

}
