package org.scache.util

/**
 * Created by frankfzw on 16-8-4.
 */

import java.net.URLClassLoader

import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import org.slf4j.{Logger, LoggerFactory}


private[scache] trait Logging {

  // Make logger transient to avoid serialized inside a Object with Logging
  @transient private var log_ : Logger = null

  // Log dir for external init
  protected var _logDir: String = null

  // Method to get the logger name for this object
  protected def logName = {
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")
  }

  // Method to get or create the logger for this object
  protected def log: Logger = {
    if (log_ == null) {
      initializeLogIfNecessary()
      log_ = LoggerFactory.getLogger(logName)
    }
    log_
  }

  // Log methods that take only a String
  protected def logInfo(msg: => String) {
    if (log.isInfoEnabled) log.info(msg)
  }

  protected def logDebug(msg: => String) {
    if (log.isDebugEnabled) log.debug(msg)
  }

  protected def logTrace(msg: => String) {
    if (log.isTraceEnabled) log.trace(msg)
  }

  protected def logWarning(msg: => String) {
    if (log.isWarnEnabled) log.warn(msg)
  }

  protected def logError(msg: => String) {
    if (log.isErrorEnabled) log.error(msg)
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  protected def logInfo(msg: => String, throwable: Throwable) {
    if (log.isInfoEnabled) log.info(msg, throwable)
  }

  protected def logDebug(msg: => String, throwable: Throwable) {
    if (log.isDebugEnabled) log.debug(msg, throwable)
  }

  protected def logTrace(msg: => String, throwable: Throwable) {
    if (log.isTraceEnabled) log.trace(msg, throwable)
  }

  protected def logWarning(msg: => String, throwable: Throwable) {
    if (log.isWarnEnabled) log.warn(msg, throwable)
  }

  protected def logError(msg: => String, throwable: Throwable) {
    if (log.isErrorEnabled) log.error(msg, throwable)
  }

  protected def isTraceEnabled(): Boolean = {
    log.isTraceEnabled
  }

  protected def initializeLogIfNecessary(): Unit = {
    if (!Logging.initialized) {
      Logging.initLock.synchronized {
        if (!Logging.initialized) {
          initializeLogging()
        }
      }
    }
  }

  private def initializeLogging(): Unit = {
    val scache_home = {
      if (_logDir == null) {
        sys.env.get("SCACHE_HOME").getOrElse("/home/spark/SCache")
      } else {
        _logDir
      }
    }
    System.setProperty("SCACHE_HOME", scache_home)
    val propertiesPath = scache_home + "/conf/log4j.properties"
    // val loader = getClass.getClassLoader
    // val url = loader.getResource("log4j.properties")
    PropertyConfigurator.configure(propertiesPath)
  }
}

private object Logging {
  @volatile private var initialized = false
  val initLock = new Object()
}
