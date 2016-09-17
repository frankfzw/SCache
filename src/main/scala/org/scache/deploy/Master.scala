package org.scache.deploy

/**
 * Created by frankfzw on 16-8-4.
 */

import org.scache.deploy.DeployMessages.Heartbeat
import org.scache.network.netty.NettyBlockTransferService
import org.scache.scheduler.LiveListenerBus
import org.scache.storage.memory.{UnifiedMemoryManager, StaticMemoryManager, MemoryManager}
import org.scache.{MapOutputTrackerMasterEndpoint, MapOutputTracker, MapOutputTrackerMaster}
import org.scache.rpc.{RpcEndpoint, ThreadSafeRpcEndpoint, RpcEndpointRef, RpcEnv}
import org.scache.serializer.{SerializerManager, JavaSerializer}
import org.scache.storage.{BlockManager, BlockManagerMasterEndpoint, BlockManagerMaster}
import org.scache.util.{RpcUtils, Logging, ScacheConf}

private class Master(
    val rpcEnv: RpcEnv,
    val hostname: String,
    val port: Int,
    conf: ScacheConf,
    isDriver: Boolean = true,
    isLocal: Boolean) extends ThreadSafeRpcEndpoint with Logging {
  val numUsableCores = conf.getInt("scache.cores", 1)


  conf.set("scache.master.port", rpcEnv.address.port.toString)

  val serializer = new JavaSerializer(conf)
  val serializerManager = new SerializerManager(serializer, conf)

  val mapOutputTracker = new MapOutputTrackerMaster(conf, isLocal)
  mapOutputTracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
    new MapOutputTrackerMasterEndpoint(rpcEnv, mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))
  logInfo("Registering " + MapOutputTracker.ENDPOINT_NAME)

  val useLegacyMemoryManager = conf.getBoolean("scache.memory.useLegacyMode", false)
  val memoryManager: MemoryManager =
      if (useLegacyMemoryManager) {
        new StaticMemoryManager(conf, numUsableCores)
      } else {
        UnifiedMemoryManager(conf, numUsableCores)
      }

  val blockTransferService = new NettyBlockTransferService(conf, hostname, numUsableCores)


  val blockManagerMasterEndpoint = rpcEnv.setupEndpoint(BlockManagerMaster.DRIVER_ENDPOINT_NAME,
    new BlockManagerMasterEndpoint(rpcEnv, isLocal, conf))
  val blockManagerMaster = new BlockManagerMaster(blockManagerMasterEndpoint, conf, isDriver)

  val blockManager = new BlockManager(ScacheConf.DRIVER_IDENTIFIER, rpcEnv, blockManagerMaster,
    serializerManager, conf, memoryManager, mapOutputTracker, blockTransferService, numUsableCores)

  override def receive: PartialFunction[Any, Unit] = {
    case Heartbeat(id, rpcRef) =>
      logInfo(s"Receive heartbeat from ${id}: ${rpcRef}")
    case _ =>
      logError("Empty message received !")
  }


}

object Master extends Logging {

  def main(args: Array[String]): Unit = {
    logInfo("Start Master")
    val conf = new ScacheConf()
    val SYSTEM_NAME = "scache.master"
    val arguements = new MasterArguments(args, conf)
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, arguements.host, arguements.port, conf)
    val masterEndpoint = rpcEnv.setupEndpoint("Master",
      new Master(rpcEnv, SYSTEM_NAME, 6073, conf, true, arguements.isLocal))
    rpcEnv.awaitTermination()
  //   logInfo(conf.getInt("scache.memory", 1).toString)
  //   logInfo(conf.getString("scache.master", "localhost").toString)
  //   logInfo(conf.getBoolean("scache.boolean", false).toString)
  }
}
