package net.mkowalski.sparkfim.util

import net.mkowalski.sparkfim.cache.{CacheRemovePolicy, PersistenceManager}

object DriverUtil {

  def createPersistenceManager(params: Map[String, String]): PersistenceManager = {
    val cacheRemovePolicy = params.get("cacheRemovePolicy") match {
      case Some(policyName) => CacheRemovePolicy.fromName(policyName)
      case None => CacheRemovePolicy.defaultPolicy
    }
    val serializedStorage = getBooleanOrDefault(params.get("serializedStorage"), defaultValue = false)
    val allowDiskStorage = getBooleanOrDefault(params.get("allowDiskStorage"), defaultValue = false)
    PersistenceManager(cacheRemovePolicy, serializedStorage, allowDiskStorage)
  }

  private def getBooleanOrDefault(param: Option[String], defaultValue: Boolean): Boolean = {
    param match {
      case Some(value) => value.toBoolean
      case None => defaultValue
    }
  }

}
