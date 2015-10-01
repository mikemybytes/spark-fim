package net.mkowalski.sparkfim.model

class PrefixGroupExtension(val itemId: Int, val support: Int, val tidList: Array[Int]) extends Serializable {

  override def equals(other: Any): Boolean = other match {
    case that: PrefixGroupExtension =>
      (that canEqual this) &&
        itemId == that.itemId &&
        support == that.support &&
        (tidList.toSet == that.tidList.toSet)
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[PrefixGroupExtension]

  override def hashCode(): Int = {
    val state = Seq(itemId, support, tidList.toSet)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"PrefixGroupExtension(itemId=$itemId, support=$support, " +
    s"tidList=${tidList.mkString("(", ",", ")")})"

}

object PrefixGroupExtension extends Serializable {

  def apply(itemId: Int, tidList: Array[Int]) = new PrefixGroupExtension(itemId, tidList.length, tidList)

  def apply(itemId: Int, support: Int, tidList: Array[Int]) = new PrefixGroupExtension(itemId, support, tidList)

}
