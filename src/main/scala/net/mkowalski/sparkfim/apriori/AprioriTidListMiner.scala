package net.mkowalski.sparkfim.apriori

import net.mkowalski.sparkfim.model.{Item, MinSupport}
import net.mkowalski.sparkfim.util.AggUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

class AprioriTidListMiner(val minSupBc: Broadcast[MinSupport]) extends Serializable {

  def mine(inputTidWithItems: RDD[(Int, Array[Int])], candidatesBc: Broadcast[List[Item]]): RDD[(Item, Array[Int])] = {
    inputTidWithItems.flatMap { case (tid, itemIds) =>
      candidatesBc.value.filter(
        candidate => candidate supportedBy itemIds
      ).map(candidate => (candidate, tid))
    }.aggregateByKey(AggUtil.zero[Int])(AggUtil.seqOp[Int], AggUtil.combOp[Int])
      .filter {
      case (item, tids) => minSupBc.value fulfilledBy tids.length
    }.map {
      case (item, tidList) => (item, tidList.toArray)
    }
  }

}

object AprioriTidListMiner {

  def apply(minSupBc: Broadcast[MinSupport]) = new AprioriTidListMiner(minSupBc)

}
