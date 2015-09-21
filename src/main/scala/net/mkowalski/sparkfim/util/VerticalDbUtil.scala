package net.mkowalski.sparkfim.util

import net.mkowalski.sparkfim.model.FimDataTypes

object VerticalDbUtil extends FimDataTypes with Serializable {

  val invalidItemIdWithEmptyTidList: ItemIdWithTidList = (-1, Array.empty[Int])
  private val itemSeparator = '\t'
  private val tidSeparator = ' '

  def lineToItemsWithTidList(line: String): ItemIdWithTidList = {
    val split = line.split(itemSeparator)
    split match {
      case Array(itemId, tidsText) =>
        val tids = tidsText.split(tidSeparator).map(_.toInt)
        (itemId.toInt, tids)
      case Array(itemId) if StringUtil.probablyDigit(itemId) =>
        (itemId.toInt, Array.empty[Int])
      case _ => // invalid line
        // no Option type for performance reasons
        invalidItemIdWithEmptyTidList
    }
  }


}
