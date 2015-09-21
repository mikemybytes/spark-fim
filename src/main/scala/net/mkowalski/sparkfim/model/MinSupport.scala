package net.mkowalski.sparkfim.model

class MinSupport(private val minSup: Int) extends Serializable {

  require(minSup > 0, "Minimum support threshold must be greater than zero")

  def fulfilledBy(support: Int): Boolean = minSup <= support

  override def equals(other: Any): Boolean = other match {
    case that: MinSupport => (that canEqual this) && minSup == that.minSup
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[MinSupport]

  override def hashCode(): Int = {
    val state = Seq(minSup)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"MinSupport(minSup=$minSup)"

}

object MinSupport extends Serializable {

  def apply(minSup: Int) = new MinSupport(minSup)

}
