package net.mkowalski.sparkfim.util

object StringUtil {

  def probablyDigit(text: String) = text forall Character.isDigit

}
