package org.scache.util

/**
 * Created by frankfzw on 16-8-5.
 */

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

class ScacheConf extends Logging {
  val configPath = ScacheConf.scacheHome + "/conf/scache.conf"
  val config = ConfigFactory.parseFile(new File(configPath))

  def getInt(key: String, default: Int): Int = {
    if (config.hasPath(key)) {
      return config.getInt(key)
    } else {
      return default
    }
  }

  def getString(key: String, default: String): String = {
    if (config.hasPath(key)) {
      return config.getString(key)
    } else {
      return default
    }
  }

  def getDouble(key: String, default: Double): Double = {
    if (config.hasPath(key)) {
      return config.getDouble(key)
    } else {
      return default
    }
  }

  def getBoolean(key: String, default: Boolean): Boolean = {
    if (config.hasPath(key)) {
      return config.getBoolean(key)
    } else {
      return default
    }
  }

}

private[scache] object ScacheConf extends Logging {
  val scacheHome = System.getenv("SCACHE_HOME")
}
