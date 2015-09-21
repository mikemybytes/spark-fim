package net.mkowalski.sparkfim.eclat

import net.mkowalski.sparkfim.model.FimDataTypes

class FrequentSingletonDatabase(val itemIdsWithTidList: Map[Int, Array[Int]],
                                val itemIdsWithPosition: Map[Int, Int]) extends Serializable with FimDataTypes {

  def possibleExtensionsOf(itemId: Int): Iterable[ItemIdWithTidList] = {
    val itemIdPosition = itemIdsWithPosition.get(itemId).get

    itemIdsWithPosition.filter {
      case (_, anotherItemPosition) => anotherItemPosition > itemIdPosition
    }.map {
      case (anotherItemId, _) => (anotherItemId, itemIdsWithTidList.get(anotherItemId).get)
    }
  }

}

object FrequentSingletonDatabase extends Serializable with FimDataTypes {

  def apply(itemIdsWithTidList: Array[ItemIdWithTidList]) = {
    val itemIdPositions =
      itemIdsWithTidList.sorted(descendingSupportOrder).map {
        case (itemId, _) => itemId
      }.zipWithIndex.toMap

    val itemIdsTidList = itemIdsWithTidList.toMap

    new FrequentSingletonDatabase(itemIdsTidList, itemIdPositions)
  }

  private def descendingSupportOrder: Ordering[ItemIdWithTidList] = {
    val supportOrder: Ordering[ItemIdWithTidList] = Ordering.by {
      case (_, tidList: Array[Int]) => tidList.length
    }
    supportOrder.reverse
  }

}
