package net.mkowalski.sparkfim.cache

object CacheRemovePriority extends Enumeration {

  sealed abstract class Priority(val name: String) extends Serializable {

    override def toString = s"Priority($name)"

  }

  case object Low extends Priority("low")

  case object High extends Priority("high")

}
