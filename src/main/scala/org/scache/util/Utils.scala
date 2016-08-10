package org.scache.util

import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import scala.collection.immutable.HashMap

/**
 * Created by frankfzw on 16-8-10.
 */
private[scache] object Utils {

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
  def extractHostPortFromSparkUrl(scacheUrl: String): (String, Int) = {
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

}
