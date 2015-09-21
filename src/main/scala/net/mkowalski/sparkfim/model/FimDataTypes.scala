package net.mkowalski.sparkfim.model

trait FimDataTypes {

  type ItemWithSupport = (Item, Int)

  type ItemWithTidList = (Item, Array[Int])

  type ItemIdWithTidList = (Int, Array[Int])

}
