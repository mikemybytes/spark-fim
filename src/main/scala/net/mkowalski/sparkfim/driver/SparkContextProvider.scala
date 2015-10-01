package net.mkowalski.sparkfim.driver

import net.mkowalski.sparkfim.eclat.FrequentSingletonDatabase
import net.mkowalski.sparkfim.model._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object SparkContextProvider {

  private val kryoSerializableClasses: Array[Class[_]] = Array(
    // backported from Apache Spark 1.5
    // this part registers some base Scala classes for Kryo serialization
    // and should be removed in case of Spark version upgrade (>= 1.5)
    classOf[Array[(Any, Any)]],
    None.getClass,
    Nil.getClass,
    classOf[ArrayBuffer[Any]],
    // project model classes:
    classOf[FimDataTypes],
    classOf[Item],
    classOf[MinSupport],
    classOf[Prefix],
    classOf[PrefixGroup],
    classOf[PrefixGroupExtension],
    classOf[FrequentSingletonDatabase]
  )

  def provideForApp(appName: String): SparkContext = {
    val conf = confWithKryo().setAppName(appName)
    new SparkContext(conf)
  }

  private def confWithKryo(): SparkConf = {
    val conf = new SparkConf()
    conf.set("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerialzer")
    conf.registerKryoClasses(kryoSerializableClasses)
    conf
  }

  def provideForTest(appName: String): SparkContext = {
    val conf = confWithKryo()
      .setAppName(appName)
      .setMaster("local[2]")
    // useful properties for test purposes:
    //      .set("spark.driver.memory", "2g")
    //      .set("spark.executor.memory", "2g")
    //      .set("spark.executor.extraJavaOptions", "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps")
    new SparkContext(conf)
  }

}
