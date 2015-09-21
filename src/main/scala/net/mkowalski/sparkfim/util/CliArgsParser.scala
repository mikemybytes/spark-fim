package net.mkowalski.sparkfim.util

import scala.annotation.tailrec

object CliArgsParser {

  type ParamsList = List[(String, String)]

  private val paramPrefix = "--"

  def parse(args: Array[String], requiredParams: List[String] = List()): Map[String, String] = {
    val params = parseArgs(args.toList).toMap
    requireParams(requiredParams, params)
    params
  }

  @tailrec
  private def requireParams(requiredParams: List[String], params: Map[String, String]): Unit = {
    requiredParams match {
      case Nil => // do nothing
      case paramName :: tail =>
        require(params.isDefinedAt(paramName), s"Missing parameter: $paramName")
        requireParams(tail, params)
    }
  }

  @tailrec
  private def parseArgs(argsToProcess: List[String], paramsFound: ParamsList = List()): ParamsList = {
    argsToProcess match {
      case Nil => paramsFound
      case name :: value :: tail if isNewParam(name) =>
        val param = (toParamName(name), value)
        parseArgs(tail, param :: paramsFound)
      case _ :: tail => parseArgs(tail, paramsFound)
    }
  }

  private def isNewParam(str: String) = str.startsWith(paramPrefix)

  private def toParamName(str: String) = str.substring(paramPrefix.length)

}
