package net.mkowalski.sparkfim.eclat

import net.mkowalski.sparkfim.model._

class InitialPrefixGroupFinder(val partition: Iterator[(Int, Array[Int])],
                               val singletonsDb: FrequentSingletonDatabase,
                               val minSup: MinSupport) extends FimDataTypes with Serializable {

  def find(): Iterator[PrefixGroup] = {
    partition.flatMap {
      case (itemId, tidList) => findPrefixGroup(itemId, tidList)
    }.toStream.iterator
  }

  private def findPrefixGroup(prefixItemId: Int, prefixItemTidList: Array[Int]): Option[PrefixGroup] = {
    val extensions = singletonsDb.possibleExtensionsOf(prefixItemId).flatMap {
      case (candidateId, candidateTidList) =>
        val combinedTidList = prefixItemTidList.intersect(candidateTidList)
        val combinedSupport = combinedTidList.length

        if (minSup fulfilledBy combinedSupport) {
          Some(PrefixGroupExtension(candidateId, combinedSupport, combinedTidList))
        } else {
          None
        }
    }

    extensions match {
      case Nil => None
      case _ => Some(PrefixGroup(Prefix(prefixItemId), extensions.toList))
    }
  }

}

object InitialPrefixGroupFinder {

  def apply(partition: Iterator[(Int, Array[Int])], singletonsDb: FrequentSingletonDatabase, minSup: MinSupport) =
    new InitialPrefixGroupFinder(partition, singletonsDb, minSup)

}
