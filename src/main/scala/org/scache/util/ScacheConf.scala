package org.scache.util

/**
 * Created by frankfzw on 16-8-5.
 */

import java.io.File
import java.util.NoSuchElementException
import java.util.concurrent.TimeUnit
import java.util.concurrent.ConcurrentHashMap

import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConverters._

class ScacheConf(var home: String) extends Logging {

  val settings = new ConcurrentHashMap[String, String]()
  private def _conf: ScacheConf = {
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
            settings.put(e.getKey, e.getValue.toString)
          }
          ScacheConf.conf = this
          ScacheConf.scacheHome = home
        }
      }
    }
    ScacheConf.conf
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
    _conf.settings.put(key, value)
    this
  }

  def getInt(key: String, default: Int): Int = {
    if (_conf.settings.containsKey(key)) {
      return _conf.settings.get(key).toInt
    } else {
      return default
    }
  }

  def getString(key: String, default: String): String = {
    if (_conf.settings.containsKey(key)) {
      return _conf.settings.get(key)
    } else {
      return default
    }
  }

  def getString(key: String): String = {
    if (_conf.settings.containsKey(key)) {
      return _conf.settings.get(key)
    } else {
      throw new NoSuchElementException(key)
    }
  }

  def getDouble(key: String, default: Double): Double = {
    if (_conf.settings.containsKey(key)) {
      return _conf.settings.get(key).toDouble
    } else {
      return default
    }
  }

  def getLong(key: String, default: Long): Long = {
    if (_conf.settings.containsKey(key)) {
      return _conf.settings.get(key).toLong
    } else {
      return default
    }
  }

  def getBoolean(key: String, default: Boolean): Boolean = {
    if (_conf.settings.containsKey(key)) {
      return _conf.settings.get(key).toBoolean
    } else {
      return default
    }
  }

  def getTimeAsMs(key: String, default: String): Long = {
    if (_conf.settings.containsKey(key)) {
      return Utils.timeStringAs(_conf.settings.get(key), TimeUnit.MILLISECONDS)
    } else {
      return Utils.timeStringAs(default, TimeUnit.MILLISECONDS)
    }
  }

  def getTimeAsSeconds(key: String, default: String): Long = {
    if (_conf.settings.containsKey(key)) {
      return Utils.timeStringAs(_conf.settings.get(key), TimeUnit.SECONDS)
    } else {
      return Utils.timeStringAs(default, TimeUnit.SECONDS)
    }
  }

  def getSizeAsBytes(key: String, defaultValue: String): Long = {
    if (_conf.settings.containsKey(key)) {
      return Utils.byteStringAsBytes(_conf.settings.get(key))
    } else {
      return Utils.byteStringAsBytes(defaultValue)
    }
  }

  def getSizeAsKb(key: String, defaultValue: String): Long = {
    if (_conf.settings.containsKey(key)) {
      return Utils.byteStringAsKb(settings.get(key))
    } else {
      return Utils.byteStringAsKb(defaultValue)
    }
  }

  def getSizeAsMb(key: String, defaultValue: String): Long = {
    if (_conf.settings.containsKey(key)) {
      return Utils.byteStringAsMb(_conf.settings.get(key))
    } else {
      return Utils.byteStringAsMb(defaultValue)
    }
  }

  def getAll(): Array[(String, String)] = {
    _conf.settings.entrySet().asScala.map(e => (e.getKey, e.getValue)).toArray
  }

  private final val avroNamespace = "avro.schema."

  def getAvroSchema: Map[Long, String] = {
    getAll.filter { case (k, v) => k.startsWith(avroNamespace) }
      .map { case (k, v) => (k.substring(avroNamespace.length).toLong, v) }
      .toMap
  }

  def getAppId: String = {
    _conf.settings.get("scache.app.id")
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
  var scacheHome: String = null
  var scacheLocalDir = scacheHome + "/tmp"
  private val lock = new Object


  def getConf(): ScacheConf = {
    return conf
  }



  private[scache] val DRIVER_IDENTIFIER = "driver"
}
