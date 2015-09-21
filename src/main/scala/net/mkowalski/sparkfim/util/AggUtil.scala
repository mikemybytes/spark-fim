package net.mkowalski.sparkfim.util

import scala.collection.mutable.ArrayBuffer

object AggUtil extends Serializable {

  def zero[T]: ArrayBuffer[T] = {
    new ArrayBuffer[T]()
  }

  def seqOp[T]: (ArrayBuffer[T], T) => ArrayBuffer[T] = {
    (buffer, element) => buffer += element
  }

  def combOp[T]: (ArrayBuffer[T], ArrayBuffer[T]) => ArrayBuffer[T] = {
    (buffer, anotherBuffer) => buffer ++= anotherBuffer
  }

}
