package net.mkowalski.sparkfim.eclat

import net.mkowalski.sparkfim.model.{FimDataTypes, Item, MinSupport, PrefixGroup}

import scala.annotation.tailrec

class EclatLocalPrefixGroupMiner(val initialPrefixGroup: PrefixGroup,
                                 val minSup: MinSupport,
                                 val depthLimit: Option[Int] = None) extends FimDataTypes with Serializable {

  if (depthLimit.isDefined) {
    require(depthLimit.get > 0, "Depth limit must be greater than zero")
  }

  def mine(withDiffsets: Boolean = true): Iterator[ItemWithSupport] = {
    val result = mineWithParams(withDiffsets, withNextGroups = false)
    result match {
      case (itemsWithSupport, _) => itemsWithSupport.iterator
    }
  }

  def mineWithNextGroups(withDiffsets: Boolean = true): Iterator[(List[ItemWithSupport], List[PrefixGroup])] = {
    val result = mineWithParams(withDiffsets, withNextGroups = true)
    result match {
      case (itemsWithSupport, nextGroups) => List(Pair(itemsWithSupport, nextGroups.get)).iterator
    }
  }

  private def mineWithParams(withDiffsets: Boolean = true, withNextGroups: Boolean): (List[ItemWithSupport], Option[List[PrefixGroup]]) = {
    initialPrefixGroup.extensions match {
      case Nil => emptyResult
      case head :: Nil => emptyResult
      case _ =>
        val nextGroups = withNextGroups match {
          case true => Some(List())
          case _ => None
        }
        mineGroups(List(initialPrefixGroup), withDiffsets, nextGroups)
    }
  }

  private def exceedsLimit(prefixGroup: PrefixGroup): Boolean = {
    depthLimit match {
      case None => false
      case limit => prefixGroup.prefix.length + 1 >= depthLimit.get
    }
  }

  @tailrec
  private def mineGroups(prefixGroups: List[PrefixGroup],
                         switchToDiffsets: Boolean,
                         nextGroups: Option[List[PrefixGroup]],
                         fisFound: List[ItemWithSupport] = List(),
                         combiner: PrefixGroupCombiner = PrefixGroupTidListCombiner(minSup)): (List[ItemWithSupport], Option[List[PrefixGroup]]) = {

    def mineGroup(prefixGroup: PrefixGroup): (List[PrefixGroup], List[ItemWithSupport]) = {
      val extensions = prefixGroup.extensions.zipWithIndex
      val groupPartialResult = extensions.map { case (extension, index) =>
        val prefix = prefixGroup.prefix.extendWith(extension.itemId)
        val groupExtensions = extensions.filter { case (_, idx) => idx > index }.flatMap {
          case (anotherExtension, _) => combiner.combine(extension, anotherExtension)
        }
        val fis = groupExtensions.map(
          extension => Item(prefix.extendWith(extension.itemId)) -> extension.support
        )
        (PrefixGroup(prefix, groupExtensions), fis)
      }

      val prefixGroupsNew: List[PrefixGroup] = groupPartialResult.map {
        case (newPrefixGroup, _) => newPrefixGroup
      }.filter(prefixGroup => prefixGroup.extensions.size > 1)
      val fisFoundNew: List[ItemWithSupport] = groupPartialResult.flatMap {
        case (_, newPrefixGroupFis) => newPrefixGroupFis
      }
      (prefixGroupsNew, fisFoundNew)
    }

    def nextStageCombiner: PrefixGroupCombiner = {
      switchToDiffsets match {
        case true => PrefixGroupDiffsetCombiner(minSup)
        case _ => combiner
      }
    }

    def nextGroupsWith(prefixGroup: PrefixGroup): Option[List[PrefixGroup]] = {
      nextGroups match {
        case None => None
        case Some(next) => Some(prefixGroup :: next)
      }
    }

    prefixGroups match {
      case Nil => (fisFound, nextGroups)
      case head :: tail if exceedsLimit(head) =>
        mineGroups(tail,
          switchToDiffsets,
          nextGroupsWith(head),
          fisFound,
          nextStageCombiner
        )
      case head :: tail =>
        mineGroup(head) match {
          case (headPrefixGroupsFound, headFisFound) =>
            mineGroups(headPrefixGroupsFound ::: tail,
              switchToDiffsets,
              nextGroups,
              headFisFound ::: fisFound,
              nextStageCombiner
            )
        }
    }

  }

  private def emptyResult = (List(), Some(List()))

}

object EclatLocalPrefixGroupMiner {

  def apply(prefixGroup: PrefixGroup, minSup: MinSupport) = new EclatLocalPrefixGroupMiner(prefixGroup, minSup)

  def apply(prefixGroup: PrefixGroup, depthLimit: Int, minSup: MinSupport) =
    new EclatLocalPrefixGroupMiner(prefixGroup, minSup, Some(depthLimit))

}
