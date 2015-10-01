package net.mkowalski.sparkfim.eclat

import net.mkowalski.sparkfim.model.{MinSupport, PrefixGroupExtension}

case class PrefixGroupDiffsetCombiner(minSup: MinSupport) extends PrefixGroupCombiner(minSup) {

  override def combine(extension: PrefixGroupExtension,
                       another: PrefixGroupExtension): Iterable[PrefixGroupExtension] = {

    val common = extension.tidList.intersect(another.tidList).toSet
    val tidList = another.tidList.filterNot(common)
    val support = extension.support - tidList.length

    support match {
      case _ if minSup fulfilledBy support => Some(PrefixGroupExtension(another.itemId, support, tidList))
      case default => None
    }
  }

}