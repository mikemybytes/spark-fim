package net.mkowalski.sparkfim.cache

import org.apache.spark.rdd.RDD

import scala.language.implicitConversions

class PersistenceRddExtension[T](val rdd: RDD[T]) {

  def cacheWith(persistenceManager: PersistenceManager): RDD[T] = persistenceManager.cache(rdd)

}

object PersistenceRddExtension {

  implicit def addPersistenceExtension[T](rdd: RDD[T]): PersistenceRddExtension[T] =
    new PersistenceRddExtension[T](rdd)

}
