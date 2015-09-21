package net.mkowalski.sparkfim.eclat

import net.mkowalski.sparkfim.model.{MinSupport, PrefixGroupExtension}

case class PrefixGroupTidListCombiner(minSup: MinSupport) extends PrefixGroupCombiner(minSup) {

  override def combine(extension: PrefixGroupExtension,
                       another: PrefixGroupExtension): Iterable[PrefixGroupExtension] = {

    val tidList = extension.tidList.intersect(another.tidList)
    val support = tidList.length

    support match {
      case _ if minSup fulfilledBy support => Some(PrefixGroupExtension(another.itemId, support, tidList))
      case default => None
    }

  }

}
