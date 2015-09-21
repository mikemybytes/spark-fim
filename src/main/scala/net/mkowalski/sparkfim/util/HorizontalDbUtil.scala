package net.mkowalski.sparkfim.util

object HorizontalDbUtil extends Serializable {

  val invalidTidWithItemIds = (-1, Array.empty[Int])
  private val tidSeparator = '\t'
  private val itemIdSeparator = ' '

  def lineToItems(line: String): Array[Int] = {
    line.split(tidSeparator) match {
      case Array(_, itemIdsText) => toItemIds(itemIdsText)
    }
  }

  def lineToTidWithItems(line: String): (Int, Array[Int]) = {
    line.split(tidSeparator) match {
      case Array(transactionId, itemIdsText) =>
        (transactionId.toInt, toItemIds(itemIdsText))
      case Array(transactionId) if StringUtil.probablyDigit(transactionId) =>
        (transactionId.toInt, Array.empty[Int])
      case _ => // invalid line
        // no Option type for performance reasons
        invalidTidWithItemIds
    }
  }

  private def toItemIds(itemIdsText: String): Array[Int] = {
    itemIdsText.split(itemIdSeparator).map(itemId => itemId.toInt)
  }

}
