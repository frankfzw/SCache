
/**
 * Created by frankfzw on 16-8-9.
 */

package org.scache.util

import org.scache.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcTimeout}

private[scache] object RpcUtils {

  private val MAX_MESSAGE_SIZE_IN_MB = Int.MaxValue / 1024 / 1024

  private def checkHost(host: String, message: String = ""): Unit = {
    assert(host.indexOf(':') == -1, message)
  }


  /**
   * Retrieve a [[RpcEndpointRef]] which is located in the driver via its name.
   */
  def makeDriverRef(name: String, conf: ScacheConf, rpcEnv: RpcEnv): RpcEndpointRef = {
    val driverHost: String = conf.getString("scache.driver.host", "localhost")
    val driverPort: Int = conf.getInt("scache.driver.port", 6388)
    checkHost(driverHost, "Expected hostname")
    rpcEnv.setupEndpointRef(RpcAddress(driverHost, driverPort), name)
  }

  /** Returns the configured number of times to retry connecting */
  def numRetries(conf: ScacheConf): Int = {
    conf.getInt("scache.rpc.numRetries", 3)
  }

  /** Returns the configured number of milliseconds to wait on each retry */
  def retryWaitMs(conf: ScacheConf): Long = {
    conf.getTimeAsMs("scache.rpc.retry.wait", "3s")
  }

  /** Returns the default Spark timeout to use for RPC ask operations. */
  def askRpcTimeout(conf: ScacheConf): RpcTimeout = {
    RpcTimeout(conf, Seq("scache.rpc.askTimeout", "scache.network.timeout"), "120s")
  }

  /** Returns the default Spark timeout to use for RPC remote endpoint lookup. */
  def lookupRpcTimeout(conf: ScacheConf): RpcTimeout = {
    RpcTimeout(conf, Seq("scache.rpc.lookupTimeout", "scache.network.timeout"), "120s")
  }



  /** Returns the configured max message size for messages in bytes. */
  def maxMessageSizeBytes(conf: ScacheConf): Int = {
    val maxSizeInMB = conf.getInt("scache.rpc.message.maxSize", 128)
    if (maxSizeInMB > MAX_MESSAGE_SIZE_IN_MB) {
      throw new IllegalArgumentException(
        s"scache.rpc.message.maxSize should not be greater than $MAX_MESSAGE_SIZE_IN_MB MB")
    }
    maxSizeInMB * 1024 * 1024
  }

}
