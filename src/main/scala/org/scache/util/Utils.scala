package org.scache.util

import java.io.{File, IOException}
import java.net._
import java.util.{UUID, Locale}
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import org.scache.network.util.JavaUtils
import org.eclipse.jetty.util.MultiException
import scala.collection.JavaConverters._

import scala.util.control.{ControlThrowable, NonFatal}

/**
 * Created by frankfzw on 16-8-10.
 */
private[scache] object Utils extends Logging{

  private val timeSuffixes = Map(
      "us" -> TimeUnit.MICROSECONDS,
      "ms" -> TimeUnit.MILLISECONDS,
      "s" -> TimeUnit.SECONDS,
      "m" -> TimeUnit.MINUTES,
      "h" -> TimeUnit.HOURS,
      "d" -> TimeUnit.DAYS
    )

  /**
   * Return a pair of host and port extracted from the `scacheUrl`.
   *
   * A scache url (`scache://host:port`) is a special URI that its scheme is `scache` and only contains
   * host and port.
   *
   * @throws Exception if `scacheUrl` is invalid.
   */
  def extractHostPortFromScacheUrl(scacheUrl: String): (String, Int) = {
    val uri = new java.net.URI(scacheUrl)
    val host = uri.getHost
    val port = uri.getPort
    if (uri.getScheme != "scache" ||
      host == null ||
      port < 0 ||
      (uri.getPath != null && !uri.getPath.isEmpty) || // uri.getPath returns "" instead of null
      uri.getFragment != null ||
      uri.getQuery != null ||
      uri.getUserInfo != null) {
      throw new Exception("Invalid master URL: " + scacheUrl)
    }
    (host, port)

  }

  /**
   * A file name may contain some invalid URI characters, such as " ". This method will convert the
   * file name to a raw path accepted by `java.net.URI(String)`.
   *
   * Note: the file name must not contain "/" or "\"
   */
  def encodeFileNameToURIRawPath(fileName: String): String = {
    require(!fileName.contains("/") && !fileName.contains("\\"))
    // `file` and `localhost` are not used. Just to prevent URI from parsing `fileName` as
    // scheme or host. The prefix "/" is required because URI doesn't accept a relative path.
    // We should remove it after we get the raw path.
    new URI("file", null, "localhost", -1, "/" + fileName, null, null).getRawPath.substring(1)
  }


  def timeStringAs(str: String, unit: TimeUnit): Long = {
    val lower = str.toLowerCase.trim
    val m = Pattern.compile("(-?[0-9]+)([a-z]+)?").matcher(lower)
    if (m.matches()) {
      throw new Exception("Failed to parse time string " + str)
    }
    val timeValue = m.group(1).toLong
    val suffix = m.group(2)

    if (suffix != null && !timeSuffixes.contains(suffix)) {
      throw new Exception("Invalid suffix: " + suffix)
    }

    if (suffix == null) {
      return unit.convert(timeValue, unit)
    } else {
      return unit.convert(timeValue, timeSuffixes.getOrElse(suffix, unit))
    }
  }


    /**
   * Execute a block of code, then a finally block, but if exceptions happen in
   * the finally block, do not suppress the original exception.
   *
   * This is primarily an issue with `finally { out.close() }` blocks, where
   * close needs to be called to clean up `out`, but if an exception happened
   * in `out.write`, it's likely `out` may be corrupted and `out.close` will
   * fail as well. This would then suppress the original/likely more meaningful
   * exception from the original `out.write` call.
   */
  def tryWithSafeFinally[T](block: => T)(finallyBlock: => Unit): T = {
    var originalThrowable: Throwable = null
    try {
      block
    } catch {
      case t: Throwable =>
        // Purposefully not using NonFatal, because even fatal exceptions
        // we don't want to have our finallyBlock suppress
        originalThrowable = t
        throw originalThrowable
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable =>
          if (originalThrowable != null) {
            originalThrowable.addSuppressed(t)
            logWarning(s"Suppressing exception in finally: " + t.getMessage, t)
            throw originalThrowable
          } else {
            throw t
          }
      }
    }
  }

  /**
   * Execute a block of code that returns a value, re-throwing any non-fatal uncaught
   * exceptions as IOException. This is used when implementing Externalizable and Serializable's
   * read and write methods, since Java's serializer will not report non-IOExceptions properly;
   * see SPARK-4080 for more context.
   */
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        logError("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        logError("Exception encountered", e)
        throw new IOException(e)
    }
  }

  def tryOrStopScache(sf: ScacheConf)(block: => Unit) {
    try {
      block
    } catch {
      case e: Exception =>
        if (sf != null) {
          sf.stop()
        } else {
          logError("Stop Scache now!!")
        }
    }
  }


  def getScacheClassLoader: ClassLoader = getClass.getClassLoader

  def getContextOrScacheClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getScacheClassLoader)

  /** Preferred alternative to Class.forName(className) */
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrScacheClassLoader)
    // scalastyle:on classforname
  }

  def byteStringAsBytes(str: String): Long = {
    JavaUtils.byteStringAsBytes(str)
  }

  def byteStringAsKb(str: String): Long = {
    JavaUtils.byteStringAsKb(str)
  }

  def byteStringAsMb(str: String): Long = {
    JavaUtils.byteStringAsMb(str)
  }

  /**
   * Maximum number of retries when binding to a port before giving up.
   */
  def portMaxRetries(conf: ScacheConf): Int = {
    val maxRetries = conf.getInt("scache.max.retries", 5)
    maxRetries
  }

  /**
   * Return whether the exception is caused by an address-port collision when binding.
   */
  def isBindCollision(exception: Throwable): Boolean = {
    exception match {
      case e: BindException =>
        if (e.getMessage != null) {
          return true
        }
        isBindCollision(e.getCause)
      case e: MultiException =>
        e.getThrowables.asScala.exists(isBindCollision)
      case e: Exception => isBindCollision(e.getCause)
      case _ => false
    }
  }

  /**
   * Attempt to start a service on the given port, or fail after a number of attempts.
   * Each subsequent attempt uses 1 + the port used in the previous attempt (unless the port is 0).
   *
   * @param startPort The initial port to start the service on.
   * @param startService Function to start service on a given port.
   *                     This is expected to throw java.net.BindException on port collision.
   * @param conf A ScacheConf used to get the maximum number of retries when binding to a port.
   * @param serviceName Name of the service.
   * @return (service: T, port: Int)
   */
  def startServiceOnPort[T](
      startPort: Int,
      startService: Int => (T, Int),
      conf: ScacheConf,
      serviceName: String = ""): (T, Int) = {

    require(startPort == 0 || (1024 <= startPort && startPort < 65536),
      "startPort should be between 1024 and 65535 (inclusive), or 0 for a random free port.")

    val serviceString = if (serviceName.isEmpty) "" else s" '$serviceName'"
    val maxRetries = portMaxRetries(conf)
    for (offset <- 0 to maxRetries) {
      // Do not increment port if startPort is 0, which is treated as a special port
      val tryPort = if (startPort == 0) {
        startPort
      } else {
        // If the new port wraps around, do not try a privilege port
        ((startPort + offset - 1024) % (65536 - 1024)) + 1024
      }
      try {
        val (service, port) = startService(tryPort)
        logInfo(s"Successfully started service$serviceString on port $port.")
        return (service, port)
      } catch {
        case e: Exception if isBindCollision(e) =>
          if (offset >= maxRetries) {
            val exceptionMessage = s"${e.getMessage}: Service$serviceString failed after " +
              s"$maxRetries retries! Consider explicitly setting the appropriate port for the " +
              s"service$serviceString (for example spark.ui.port for SparkUI) to an available " +
              "port or increasing spark.port.maxRetries."
            val exception = new BindException(exceptionMessage)
            // restore original stack trace
            exception.setStackTrace(e.getStackTrace)
            throw exception
          }
          logWarning(s"Service$serviceString could not bind on port $tryPort. " +
            s"Attempting port ${tryPort + 1}.")
      }
    }
    // Should never happen
    throw new Exception(s"Failed to start service$serviceString on port $startPort")
  }

  def checkHost(host: String, message: String = "") {
    assert(host.indexOf(':') == -1, message)
  }

  def bytesToString(size: Long): String = {
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    val (value, unit) = {
      if (size >= 2*TB) {
        (size.asInstanceOf[Double] / TB, "TB")
      } else if (size >= 2*GB) {
        (size.asInstanceOf[Double] / GB, "GB")
      } else if (size >= 2*MB) {
        (size.asInstanceOf[Double] / MB, "MB")
      } else if (size >= 2*KB) {
        (size.asInstanceOf[Double] / KB, "KB")
      } else {
        (size.asInstanceOf[Double], "B")
      }
    }
    "%.1f %s".formatLocal(Locale.US, value, unit)
  }

  /**
   * Return the string to tell how long has passed in milliseconds.
   */
  def getUsedTimeMs(startTimeMs: Long): String = {
    " " + (System.currentTimeMillis - startTimeMs) + " ms"
  }

  def nonNegativeHash(obj: AnyRef): Int = {

    // Required ?
    if (obj eq null) return 0

    val hash = obj.hashCode
    // math.abs fails for Int.MinValue
    val hashAbs = if (Int.MinValue != hash) math.abs(hash) else 0

    // Nothing else to guard against ?
    hashAbs
  }


  /**
   * Create a directory inside the given parent directory. The directory is guaranteed to be
   * newly created, and is not marked for automatic deletion.
   */
  def createDirectory(root: String, namePrefix: String = "scache"): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: SecurityException => dir = null; }
    }

    dir.getCanonicalFile
  }

  /**
   * Check to see if file is a symbolic link.
   */
  def isSymlink(file: File): Boolean = {
    if (file == null) throw new NullPointerException("File must not be null")
    val fileInCanonicalDir = if (file.getParent() == null) {
      file
    } else {
      new File(file.getParentFile().getCanonicalFile(), file.getName())
    }

    !fileInCanonicalDir.getCanonicalFile().equals(fileInCanonicalDir.getAbsoluteFile())
  }

  private def listFilesSafely(file: File): Seq[File] = {
    if (file.exists()) {
      val files = file.listFiles()
      if (files == null) {
        throw new IOException("Failed to list files for dir: " + file)
      }
      files
    } else {
      List()
    }
  }

  /**
   * Delete a file or directory and its contents recursively.
   * Don't follow directories if they are symlinks.
   * Throws an exception if deletion is unsuccessful.
   */
  def deleteRecursively(file: File) {
    if (file != null) {
      try {
        if (file.isDirectory && !isSymlink(file)) {
          var savedIOException: IOException = null
          for (child <- listFilesSafely(file)) {
            try {
              deleteRecursively(child)
            } catch {
              // In case of multiple exceptions, only last one will be thrown
              case ioe: IOException => savedIOException = ioe
            }
          }
          if (savedIOException != null) {
            throw savedIOException
          }
          ShutdownHookManager.removeShutdownDeleteDir(file)
        }
      } finally {
        if (!file.delete()) {
          // Delete can also fail if the file simply did not exist
          if (file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath)
          }
        }
      }
    }
  }

  /**
   * Execute the given block, logging and re-throwing any uncaught exception.
   * This is particularly useful for wrapping code that runs in a thread, to ensure
   * that exceptions are printed, and to avoid having to catch Throwable.
   */
  def logUncaughtExceptions[T](f: => T): T = {
    try {
      f
    } catch {
      case ct: ControlThrowable =>
        throw ct
      case t: Throwable =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        throw t
    }
  }

  def findLocalInetAddress(): InetAddress = {
    val defaultIpOverride = System.getenv("SCACHE_LOCAL_IP")
    if (defaultIpOverride != null) {
      InetAddress.getByName(defaultIpOverride)
    } else {
      val address = InetAddress.getLocalHost
      if (address.isLoopbackAddress) {
        // Address resolves to something like 127.0.1.1, which happens on Debian; try to find
        // a better address using the local network interfaces
        // getNetworkInterfaces returns ifs in reverse order compared to ifconfig output order
        // on unix-like system. On windows, it returns in index order.
        // It's more proper to pick ip address following system output order.
        val activeNetworkIFs = NetworkInterface.getNetworkInterfaces.asScala.toSeq
        val reOrderedNetworkIFs = activeNetworkIFs.reverse

        for (ni <- reOrderedNetworkIFs) {
          val addresses = ni.getInetAddresses.asScala
            .filterNot(addr => addr.isLinkLocalAddress || addr.isLoopbackAddress).toSeq
          if (addresses.nonEmpty) {
            val addr = addresses.find(_.isInstanceOf[Inet4Address]).getOrElse(addresses.head)
            // because of Inet6Address.toHostName may add interface at the end if it knows about it
            val strippedAddress = InetAddress.getByAddress(addr.getAddress)
            // We've found an address that looks reasonable!
            logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
              " a loopback address: " + address.getHostAddress + "; using " +
              strippedAddress.getHostAddress + " instead (on interface " + ni.getName + ")")
            logWarning("Set SCACHE_LOCAL_IP if you need to bind to another address")
            return strippedAddress
          }
        }
        logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
          " a loopback address: " + address.getHostAddress + ", but we couldn't find any" +
          " external IP address!")
        logWarning("Set SCACHE_LOCAL_IP if you need to bind to another address")
      }
      address
    }
  }


}
