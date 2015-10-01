package net.mkowalski.sparkfim.eclat

import net.mkowalski.sparkfim.model.{Prefix, PrefixGroup, PrefixGroupExtension}
import net.mkowalski.sparkfim.test.BaseTest

class PrefixGroupDiffsetConverterTest extends BaseTest {

  val inputGroupWithTidLists = PrefixGroup(
    Prefix(1, 2, 3, 4),
    List(
      PrefixGroupExtension(5, Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)),
      PrefixGroupExtension(6, Array(2, 4, 6, 8, 10)),
      PrefixGroupExtension(7, Array(1, 2, 4, 5, 6, 8, 9, 10)),
      PrefixGroupExtension(8, Array(1, 2, 3, 4, 5, 6, 7, 8))
    )
  )

  val expectedGroupWithDiffsets = PrefixGroup(
    Prefix(1, 2, 3, 4),
    List(
      PrefixGroupExtension(5, support = 10, Array()),
      PrefixGroupExtension(6, support = 5, Array(1, 3, 5, 7, 9)),
      PrefixGroupExtension(7, support = 8, Array(3, 7)),
      PrefixGroupExtension(8, support = 8, Array(9, 10))
    )
  )

  test("Should convert PrefixGroup (extensions) to diffsets") {
    val result = PrefixGroupDiffsetConverter.convert(inputGroupWithTidLists)
    assert(result == expectedGroupWithDiffsets)
  }

}