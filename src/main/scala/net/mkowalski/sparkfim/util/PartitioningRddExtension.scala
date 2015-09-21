package net.mkowalski.sparkfim.util

import org.apache.spark.rdd.RDD

import scala.language.implicitConversions

class PartitioningRddExtension[T](val rdd: RDD[T]) {

  def forcePartitionsNum(partitionsNum: Option[Int]): RDD[T] = {
    val currentPartitionsNum = rdd.partitions.length
    partitionsNum match {
      case Some(value) if value >= currentPartitionsNum => rdd.repartition(value)
      case _ => rdd
    }
  }

}

object PartitioningRddExtension {

  implicit def addPartitioningExtension[T](rdd: RDD[T]): PartitioningRddExtension[T] =
    new PartitioningRddExtension[T](rdd)

}
