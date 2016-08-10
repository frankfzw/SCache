package org.scache.deploy

/**
 * Created by frankfzw on 16-8-4.
 */
import org.scache.util.Logging
import org.scache.util.ScacheConf

private class Master extends Logging {
  val a = "a"
}

object Master extends Logging {

  def main(args: Array[String]): Unit = {
    logInfo("Start Master")
    val conf = new ScacheConf()
    logInfo(conf.getInt("scache.memory", 1).toString)
    logInfo(conf.getString("scache.master", "localhost").toString)
    logInfo(conf.getBoolean("scache.boolean", false).toString)
  }
}
