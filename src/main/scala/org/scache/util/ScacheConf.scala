package org.scache.util

/**
 * Created by frankfzw on 16-8-5.
 */

import java.io.File
import java.util.NoSuchElementException
import java.util.concurrent.TimeUnit
import java.util.concurrent.ConcurrentHashMap

import com.typesafe.config.{Config, ConfigFactory, ConfigValueType}

import scala.collection.JavaConverters._

class ScacheConf(var home: String) extends Logging {

  final val settings = new ConcurrentHashMap[String, String]()

  if (ScacheConf.conf == null) {
    ScacheConf.lock synchronized {
      if (ScacheConf.conf == null) {
        if (home == null) {
          logError(s"SCACHE_HOME is not set correctly")
          throw new Exception(s"SCACHE_HOME is not set correctly")
        }
        _logDir = home
        val configPath = home + "/conf/scache.conf"
        val config = ConfigFactory.parseFile(new File(configPath))
        for (e <- config.entrySet().asScala) {
          e.getValue.valueType() match {
            case ConfigValueType.BOOLEAN =>
              settings.put(e.getKey, config.getBoolean(e.getKey).toString)
            case ConfigValueType.STRING =>
              settings.put(e.getKey, config.getString(e.getKey))
            case ConfigValueType.NUMBER =>
              settings.put(e.getKey, config.getInt(e.getKey).toString)
            case _ =>
          }

        }
        ScacheConf.conf = this
        ScacheConf.scacheHome = home
      }
    }
  }


  def this() {
    this(sys.env.get("SCACHE_HOME").getOrElse("/home/spark/SCache"))
  }



  private[scache] def set(key: String, value: String, slient: Boolean = false): ScacheConf = {
    if (key == null) {
      throw new Exception("config null key")
    }

    if (value == null) {
      throw new Exception("config null value for " + key)
    }

    if (!slient) {
      logInfo(s"config set key ${key} to value ${value}")
    }
    ScacheConf.conf.settings.put(key, value)
    this
  }

  def getInt(key: String, default: Int): Int = {
    if (ScacheConf.conf.settings.containsKey(key)) {
      return ScacheConf.conf.settings.get(key).toInt
    } else {
      return default
    }
  }

  def getString(key: String, default: String): String = {
    if (ScacheConf.conf.settings.containsKey(key)) {
      return ScacheConf.conf.settings.get(key)
    } else {
      return default
    }
  }

  def getString(key: String): String = {
    if (ScacheConf.conf.settings.containsKey(key)) {
      return ScacheConf.conf.settings.get(key)
    } else {
      throw new NoSuchElementException(key)
    }
  }

  def getDouble(key: String, default: Double): Double = {
    if (ScacheConf.conf.settings.containsKey(key)) {
      return ScacheConf.conf.settings.get(key).toDouble
    } else {
      return default
    }
  }

  def getLong(key: String, default: Long): Long = {
    if (ScacheConf.conf.settings.containsKey(key)) {
      return ScacheConf.conf.settings.get(key).toLong
    } else {
      return default
    }
  }

  def getBoolean(key: String, default: Boolean): Boolean = {
    if (ScacheConf.conf.settings.containsKey(key)) {
      return ScacheConf.conf.settings.get(key).toBoolean
    } else {
      return default
    }
  }

  def getTimeAsMs(key: String, default: String): Long = {
    if (ScacheConf.conf.settings.containsKey(key)) {
      return Utils.timeStringAs(ScacheConf.conf.settings.get(key), TimeUnit.MILLISECONDS)
    } else {
      return Utils.timeStringAs(default, TimeUnit.MILLISECONDS)
    }
  }

  def getTimeAsSeconds(key: String, default: String): Long = {
    if (ScacheConf.conf.settings.containsKey(key)) {
      return Utils.timeStringAs(ScacheConf.conf.settings.get(key), TimeUnit.SECONDS)
    } else {
      return Utils.timeStringAs(default, TimeUnit.SECONDS)
    }
  }

  def getSizeAsBytes(key: String, defaultValue: String): Long = {
    if (ScacheConf.conf.settings.containsKey(key)) {
      return Utils.byteStringAsBytes(ScacheConf.conf.settings.get(key))
    } else {
      return Utils.byteStringAsBytes(defaultValue)
    }
  }

  def getSizeAsKb(key: String, defaultValue: String): Long = {
    if (ScacheConf.conf.settings.containsKey(key)) {
      return Utils.byteStringAsKb(settings.get(key))
    } else {
      return Utils.byteStringAsKb(defaultValue)
    }
  }

  def getSizeAsMb(key: String, defaultValue: String): Long = {
    if (ScacheConf.conf.settings.containsKey(key)) {
      return Utils.byteStringAsMb(ScacheConf.conf.settings.get(key))
    } else {
      return Utils.byteStringAsMb(defaultValue)
    }
  }

  def getAll(): Array[(String, String)] = {
    ScacheConf.conf.settings.entrySet().asScala.map(e => (e.getKey, e.getValue)).toArray
  }

  private final val avroNamespace = "avro.schema."

  def getAvroSchema: Map[Long, String] = {
    getAll.filter { case (k, v) => k.startsWith(avroNamespace) }
      .map { case (k, v) => (k.substring(avroNamespace.length).toLong, v) }
      .toMap
  }

  def getAppId: String = {
    ScacheConf.conf.settings.get("scache.app.id")
  }


  // override def clone(): ScacheConf = {
  //   val cloned = new ScacheConf()
  //   settings.entrySet().asScala.foreach {
  //     e =>
  //       cloned.set(e.getKey, e.getValue, true)
  //   }
  //   cloned
  // }

  //TODO: empty now
  def stop() = {

  }
}

object ScacheConf extends Logging {

  private var conf: ScacheConf = null
  private var scacheHome: String = null
  def scacheLocalDir: String = {scacheHome + "/tmp"}
  private val lock = new Object


  def getConf(): ScacheConf = {
    return conf
  }

  def getHome(): String = {
    scacheHome
  }


  private[scache] val DRIVER_IDENTIFIER = "driver"
}
