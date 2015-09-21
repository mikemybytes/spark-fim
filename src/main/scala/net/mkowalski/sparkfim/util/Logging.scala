package net.mkowalski.sparkfim.util

import org.apache.log4j.Logger

trait Logging {

  lazy val LOG = Logger.getLogger(this.getClass.getName)

}
