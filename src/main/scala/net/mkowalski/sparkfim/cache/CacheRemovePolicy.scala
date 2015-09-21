package net.mkowalski.sparkfim.cache

object CacheRemovePolicy extends Enumeration {

  val defaultPolicy = Aggressive

  def fromName(name: String): Policy = {
    name match {
      case Auto.name => Auto
      case Normal.name => Normal
      case Aggressive.name => Aggressive
      case Paranoid.name => Paranoid
      case _ => throw new IllegalArgumentException("Unknown cache remove policy")
    }
  }

  sealed abstract class Policy(val name: String) extends Serializable {

    override def toString = s"Policy($name)"

  }

  case object Auto extends Policy("auto")

  case object Normal extends Policy("normal")

  case object Aggressive extends Policy("aggressive")

  case object Paranoid extends Policy("paranoid")

}
