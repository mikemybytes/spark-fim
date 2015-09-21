package net.mkowalski.sparkfim.model

import net.mkowalski.sparkfim.util.PrettyPrintable

class Item(private val id: Array[Int]) extends PrettyPrintable with Serializable {

  def prefix: Prefix = new Prefix(id.drop(1))

  def merge(anotherItem: Item): Option[Item] = {
    val anotherHead = anotherItem.id.head
    this.id.head match {
      case `anotherHead` => None
      case _ => mergeOption(anotherItem)
    }
  }

  private def mergeOption(anotherItem: Item): Option[Item] = {
    val idPrefixItems = this.id.drop(1)
    if (idPrefixItems sameElements anotherItem.id.drop(1)) {
      val mergedId = Array(this.id.head, anotherItem.id.head).sorted(Ordering.Int.reverse) ++ idPrefixItems
      Some(new Item(mergedId))
    } else {
      None
    }
  }

  def supportedBy(transactionItems: Array[Int]): Boolean = {
    this.id.intersect(transactionItems).length == id.length
  }

  def extension: Int = id.head

  def idLength: Int = id.length

  def sorted(ordering: Ordering[Int] = Ordering.Int): Item = new Item(this.id.sorted(ordering.reverse))

  override def prettyPrint: String = s"Item(id=$id)"

  override def toString = id.mkString(" ")

  override def equals(other: Any): Boolean = other match {
    case that: Item =>
      (that canEqual this) && (id sameElements that.id)
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Item]

  override def hashCode(): Int = {
    val state = Seq(id.deep)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object Item extends Serializable {

  def apply(id: Int) = new Item(Array(id))

  def apply(ids: Int*) = new Item(ids.reverse.toArray)

  def apply(id: Array[Int]) = new Item(id.reverse)

  def apply(prefix: Prefix) = new Item(prefix.items)

}
