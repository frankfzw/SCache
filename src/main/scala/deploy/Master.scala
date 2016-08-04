package org.scache.deploy

/**
 * Created by frankfzw on 16-8-4.
 */
import org.scache.util.Logging

private class Master extends Logging {
  val a = "a"
}

object Master extends Logging {

  def main(args: Array[String]): Unit = {
    logInfo("Start Master")
  }
}
