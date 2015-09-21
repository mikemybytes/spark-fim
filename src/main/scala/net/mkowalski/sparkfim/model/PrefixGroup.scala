package net.mkowalski.sparkfim.model

class PrefixGroup(val prefix: Prefix,
                  val extensions: List[PrefixGroupExtension]) extends FimDataTypes with Serializable {

  def itemsWithSupport: Iterable[ItemWithSupport] = {
    this.extensions.map(extension => {
      (Item(this.prefix.extendWith(extension.itemId)), extension.support)
    })
  }

  override def equals(other: Any): Boolean = other match {
    case that: PrefixGroup => (that canEqual this) &&
      prefix == that.prefix &&
      extensions.toSet == that.extensions.toSet
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[PrefixGroup]

  override def hashCode(): Int = {
    val state = Seq(prefix, extensions)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"PrefixGroup(prefix=$prefix, extensions=$extensions)"

}

object PrefixGroup extends Serializable {

  def apply(extensions: List[PrefixGroupExtension]) = new PrefixGroup(Prefix(), extensions)

  def apply(prefix: Prefix, extensions: List[PrefixGroupExtension]) = new PrefixGroup(prefix, extensions)

}
