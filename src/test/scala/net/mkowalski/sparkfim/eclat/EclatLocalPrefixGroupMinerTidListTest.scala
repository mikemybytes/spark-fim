package net.mkowalski.sparkfim.eclat

import net.mkowalski.sparkfim.model.{Prefix, PrefixGroup, PrefixGroupExtension}

class EclatLocalPrefixGroupMinerTidListTest extends EclatLocalPrefixGroupMinerBaseTest {

  override def mineWithDiffsets: Boolean = false

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

}
