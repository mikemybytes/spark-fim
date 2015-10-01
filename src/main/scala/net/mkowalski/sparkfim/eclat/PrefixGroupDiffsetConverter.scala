package net.mkowalski.sparkfim.eclat

import net.mkowalski.sparkfim.model.{PrefixGroup, PrefixGroupExtension}

import scala.collection.mutable

object PrefixGroupDiffsetConverter {

  def convert(prefixGroup: PrefixGroup): PrefixGroup = {
    val tidsAll = findAllTids(prefixGroup)
    val diffsetExtensions = prefixGroup.extensions.map(toDiffsetExtension(_, tidsAll))
    PrefixGroup(prefixGroup.prefix, diffsetExtensions)
  }

  private def findAllTids(prefixGroup: PrefixGroup): Array[Int] = {
    val groupTidsAll: mutable.Set[Int] = mutable.HashSet()
    prefixGroup.extensions.foreach(
      extension => groupTidsAll ++= extension.tidList
    )
    groupTidsAll.toArray
  }

  private def toDiffsetExtension(extension: PrefixGroupExtension, tidsAll: Array[Int]): PrefixGroupExtension = {
    val diff: Array[Int] = tidsAll.toArray.diff(extension.tidList)
    PrefixGroupExtension(extension.itemId, extension.support, diff)
  }

}
