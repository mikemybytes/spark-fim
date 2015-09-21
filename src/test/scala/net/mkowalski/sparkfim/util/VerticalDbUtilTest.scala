package net.mkowalski.sparkfim.util

import net.mkowalski.sparkfim.test.BaseTest

class VerticalDbUtilTest extends BaseTest {

  test("lineToItemsWithTidList - no TID-list") {
    val input = "12345"
    val result = VerticalDbUtil.lineToItemsWithTidList(input)
    result match {
      case (itemId, tidList: Array[Int]) =>
        assert(itemId == 12345)
        assert(tidList.isEmpty)
      case _ => fail(s"Illegal result: $result")
    }
  }

  test("lineToItemsWithTidList - empty TID-list") {
    val input = "12345\t"
    val result = VerticalDbUtil.lineToItemsWithTidList(input)
    result match {
      case (itemId, tidList: Array[Int]) =>
        assert(itemId == 12345)
        assert(tidList.isEmpty)
      case _ => fail(s"Illegal result: $result")
    }
  }

  test("lineToItemsWithTidList - singleton TID-list") {
    val input = "12345\t321"
    val result = VerticalDbUtil.lineToItemsWithTidList(input)
    result match {
      case (itemId, tidList: Array[Int]) =>
        assert(itemId == 12345)
        assert(tidList.toSeq == Seq(321))
      case _ => fail(s"Illegal result: $result")
    }
  }

  test("lineToItemsWithTidList - multiple tids TID-list") {
    val input = "12345\t321 5 16"
    val result = VerticalDbUtil.lineToItemsWithTidList(input)
    result match {
      case (itemId, tidList: Array[Int]) =>
        assert(itemId == 12345)
        assert(tidList.toSeq == Seq(321, 5, 16))
      case _ => fail(s"Illegal result: $result")
    }
  }

  test("lineToItemsWithTidList - invalid line") {
    val input = "12345 321 5 16"
    val result = VerticalDbUtil.lineToItemsWithTidList(input)
    assert(result == VerticalDbUtil.invalidItemIdWithEmptyTidList)
  }

}
