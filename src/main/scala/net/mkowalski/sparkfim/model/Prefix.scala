package net.mkowalski.sparkfim.model

class Prefix(private val prefixItemIds: Array[Int]) extends Serializable {

  def extendWith(itemId: Int) = new Prefix(itemId +: prefixItemIds)

  def items = prefixItemIds

  def length = prefixItemIds.length

  override def equals(other: Any): Boolean = other match {
    case that: Prefix => (that canEqual this) && (prefixItemIds sameElements that.prefixItemIds)
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Prefix]

  override def hashCode(): Int = {
    val state = Seq(prefixItemIds.deep)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"Prefix(prefixItemIds=${prefixItemIds.mkString("(", ",", ")")})"

}

object Prefix extends Serializable {

  def apply() = new Prefix(Array.empty[Int])

  def apply(prefixItem: Int) = new Prefix(Array(prefixItem))

  def apply(prefixItemIds: Int*) = new Prefix(prefixItemIds.reverse.toArray)

}
