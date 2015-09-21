package net.mkowalski.sparkfim.apriori

import net.mkowalski.sparkfim.model.Item

object CandidatesGenerator extends Serializable {

  def generateFrom(previousItemsList: List[Item]): List[Item] = {
    previousItemsList.combinations(2).flatMap(combination => {
      val item1 = combination.head
      val item2 = combination.last
      item1.merge(item2)
    }).toList
  }

}
