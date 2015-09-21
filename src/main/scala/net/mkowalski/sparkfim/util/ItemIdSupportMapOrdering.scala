package net.mkowalski.sparkfim.util

class ItemIdSupportMapOrdering(val itemIdsSupportMap: Map[Int, Int]) extends Ordering[Int] with Serializable {

  override def compare(item1Id: Int, item2Id: Int): Int = {
    val item1Support = itemIdsSupportMap.get(item1Id)
    val item2Support = itemIdsSupportMap.get(item2Id)

    if (item1Support.isEmpty || item2Support.isEmpty) {
      throw new IllegalArgumentException(s"Not all of compared items found in map ($item1Id, $item2Id)")
    }

    item1Support.get compareTo item2Support.get
  }

}

object ItemIdSupportMapOrdering {

  def apply(itemIdsSupportMap: Map[Int, Int]) = new ItemIdSupportMapOrdering(itemIdsSupportMap)

}
