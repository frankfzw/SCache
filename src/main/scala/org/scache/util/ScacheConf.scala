package org.scache.util

/**
 * Created by frankfzw on 16-8-5.
 */

import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.ConcurrentHashMap

import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConverters._

class ScacheConf extends Cloneable with Logging {
  private val configPath = ScacheConf.scacheHome + "/conf/scache.conf"
  private val config = ConfigFactory.parseFile(new File(configPath))
  private val settings = new ConcurrentHashMap[String, String]()

  for (e <- config.entrySet().asScala) {
    settings.put(e.getKey, e.getValue.toString)
  }

  ScacheConf.setSingleton(this)

  private[scache] def set(key: String, value: String, slient: Boolean): ScacheConf = {
    if (key == null) {
      throw new Exception("config null key")
    }

    if (value == null) {
      throw new Exception("config null value for " + key)
    }

    if (!slient) {
      logInfo(s"config set key ${} to value ${}".format(key, value))
    }
    settings.put(key, value)
    this
  }

  def getInt(key: String, default: Int): Int = {
    if (settings.containsKey(key)) {
      return settings.get(key).toInt
    } else {
      return default
    }
  }

  def getString(key: String, default: String): String = {
    if (settings.containsKey(key)) {
      return settings.get(key)
    } else {
      return default
    }
  }

  def getDouble(key: String, default: Double): Double = {
    if (settings.containsKey(key)) {
      return settings.get(key).toDouble
    } else {
      return default
    }
  }

  def getLong(key: String, default: Long): Long = {
    if (settings.containsKey(key)) {
      return settings.get(key).toLong
    } else {
      return default
    }
  }

  def getBoolean(key: String, default: Boolean): Boolean = {
    if (settings.containsKey(key)) {
      return settings.get(key).toBoolean
    } else {
      return default
    }
  }

  def getTimeAsMs(key: String, default: String): Long = {
    if (settings.containsKey(key)) {
      return Utils.timeStringAs(settings.get(key), TimeUnit.MILLISECONDS)
    } else {
      return Utils.timeStringAs(default, TimeUnit.MILLISECONDS)
    }
  }

  def getTimeAsSeconds(key: String, default: String): Long = {
    if (settings.containsKey(key)) {
      return Utils.timeStringAs(settings.get(key), TimeUnit.SECONDS)
    } else {
      return Utils.timeStringAs(default, TimeUnit.SECONDS)
    }
  }

  def getSizeAsBytes(key: String, defaultValue: String): Long = {
    if (settings.containsKey(key)) {
      return Utils.byteStringAsBytes(settings.get(key))
    } else {
      return Utils.byteStringAsBytes(defaultValue)
    }
  }

  def getSizeAsKb(key: String, defaultValue: String): Long = {
    if (settings.containsKey(key)) {
      return Utils.byteStringAsKb(settings.get(key))
    } else {
      return Utils.byteStringAsKb(defaultValue)
    }
  }

  def getSizeAsMb(key: String, defaultValue: String): Long = {
    if (settings.containsKey(key)) {
      return Utils.byteStringAsMb(settings.get(key))
    } else {
      return Utils.byteStringAsMb(defaultValue)
    }
  }

  def getAll(): Array[(String, String)] = {
    settings.entrySet().asScala.map(e => (e.getKey, e.getValue)).toArray
  }

  private final val avroNamespace = "avro.schema."

  def getAvroSchema: Map[Long, String] = {
    getAll.filter { case (k, v) => k.startsWith(avroNamespace) }
      .map { case (k, v) => (k.substring(avroNamespace.length).toLong, v) }
      .toMap
  }

  def getAppId: String = {
    settings.get("scache.app.id")
  }


  override def clone(): ScacheConf = {
    val cloned = new ScacheConf()
    settings.entrySet().asScala.foreach {
      e =>
        cloned.set(e.getKey, e.getValue, true)
    }
    cloned
  }

  //TODO: empty now
  def stop() = {

  }
}

private[scache] object ScacheConf extends Logging {

  private def setSingleton(c: ScacheConf) = {
    if (conf == null) {
      conf = c
    } else {
      logError("Scache Conf can be init only once")
    }
  }

  def getConf(): ScacheConf = {
    return conf
  }

  private var conf: ScacheConf = null
  val scacheHome = System.getenv("SCACHE_HOME")
  val scacheLocalDir = scacheHome + "/tmp"

  private[scache] val DRIVER_IDENTIFIER = "driver"
}
