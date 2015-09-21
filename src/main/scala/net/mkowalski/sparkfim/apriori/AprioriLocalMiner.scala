package net.mkowalski.sparkfim.apriori

import net.mkowalski.sparkfim.model.{FimDataTypes, Item}

import scala.collection.mutable

case class AprioriLocalMiner(itemsIterator: Iterator[Array[Int]], previousItems: List[Item] = Nil)
  extends FimDataTypes with Serializable {

  def mine(): Iterator[ItemWithSupport] = {
    previousItems match {
      case Nil => findSingletonsWithSupport()
      case _ => findNextGeneration()
    }
  }

  private def findSingletonsWithSupport(): Iterator[ItemWithSupport] = {
    itemsIterator.flatten.toList.groupBy(identity).map {
      case (itemId, supportingTids) => (Item(itemId), supportingTids.length)
    }.iterator
  }

  private def findNextGeneration(): Iterator[ItemWithSupport] = {
    val candidatesSupport = new mutable.HashMap[Item, Int]()
    val candidatesFromPrevious = CandidatesGenerator.generateFrom(previousItems)

    itemsIterator.foreach(items => {
      candidatesFromPrevious.foreach(candidate => {
        if (candidate supportedBy items) {
          val sup = candidatesSupport.getOrElseUpdate(candidate, 0)
          candidatesSupport.update(candidate, sup + 1)
        }
      })
    })

    candidatesSupport.iterator
  }

}
