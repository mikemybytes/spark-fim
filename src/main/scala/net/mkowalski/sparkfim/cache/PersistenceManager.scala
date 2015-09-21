package net.mkowalski.sparkfim.cache

import net.mkowalski.sparkfim.cache.CacheRemovePolicy._
import net.mkowalski.sparkfim.cache.CacheRemovePriority.{High, Low, Priority}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class PersistenceManager(val cacheRemovePolicy: Policy, val serializedStorage: Boolean, val allowDiskStorage: Boolean)
  extends Serializable {

  val storageLevel = determineStorageLevel

  def cache[T](rdd: RDD[T]): RDD[T] = {
    rdd.persist(storageLevel)
  }

  def markUnused(rdd: RDD[_], removePriority: Priority): Unit = {
    cacheRemovePolicy match {
      case Auto => // do nothing (automatic Spark management)
      case Normal =>
        removePriority match {
          case Low => // do nothing (automatic Spark management)
          case High => rdd.unpersist(blocking = false)
        }
      case Aggressive =>
        removePriority match {
          case Low => rdd.unpersist(blocking = false)
          case High => rdd.unpersist(blocking = false)
        }
      case Paranoid =>
        removePriority match {
          case Low => rdd.unpersist(blocking = false)
          case High => rdd.unpersist(blocking = true)
        }
    }
  }

  def markUnused(broadcast: Broadcast[_]): Unit = {
    cacheRemovePolicy match {
      case Auto => // do nothing
      case Paranoid => broadcast.unpersist(blocking = true)
      case _ => broadcast.unpersist(blocking = false)
    }
  }

  override def toString = s"PersistenceManager(storageLevel=$printableStorageLevel, cacheRemovePolicy=$cacheRemovePolicy)"

  private def printableStorageLevel: String = {
    storageLevel match {
      case StorageLevel.MEMORY_ONLY => "MEMORY_ONLY"
      case StorageLevel.MEMORY_AND_DISK => "MEMORY_AND_DISK"
      case StorageLevel.MEMORY_ONLY_SER => "MEMORY_ONLY_SER"
      case StorageLevel.MEMORY_AND_DISK_SER => "MEMORY_AND_DISK_SER"
    }
  }

  private def determineStorageLevel: StorageLevel = {
    serializedStorage match {
      case true => allowDiskStorage match {
        case true => StorageLevel.MEMORY_AND_DISK_SER
        case _ => StorageLevel.MEMORY_ONLY_SER
      }
      case _ => allowDiskStorage match {
        case true => StorageLevel.MEMORY_AND_DISK
        case _ => StorageLevel.MEMORY_ONLY
      }
    }
  }

}

object PersistenceManager {

  def apply(cacheRemovePolicy: Policy, serializedStorage: Boolean = false, allowDiskStorage: Boolean = false) =
    new PersistenceManager(cacheRemovePolicy, serializedStorage, allowDiskStorage)

}