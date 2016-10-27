package org.scache.deploy

import org.scache.deploy.DeployMessages.{RegisterClient, RegisterClientSuccess}
import org.scache.network.netty.NettyBlockTransferService
import org.scache.storage.{ScacheBlockId, BlockManager, BlockManagerMaster, BlockManagerMasterEndpoint}
import org.scache.storage.memory.{MemoryManager, StaticMemoryManager, UnifiedMemoryManager}
import org.scache.{MapOutputTracker, MapOutputTrackerMaster, MapOutputTrackerWorker}
import org.scache.rpc._
import org.scache.serializer.{JavaSerializer, SerializerManager}
import org.scache.util._

import scala.util.{Failure, Success}

/**
 * Created by frankfzw on 16-9-19.
 */
class Client(
  val rpcEnv: RpcEnv,
  val hostname: String,
  val masterHostname: String,
  val port: Int,
  conf: ScacheConf) extends ThreadSafeRpcEndpoint with Logging {
   conf.set("scache.client.port", rpcEnv.address.port.toString)

  val numUsableCores = conf.getInt("scache.cores", 1)
  val serializer = new JavaSerializer(conf)
  val serializerManager = new SerializerManager(serializer, conf)
  var clientId: Int = -1

  @volatile var master: RpcEndpointRef = null

  val mapOutputTracker = new MapOutputTrackerWorker(conf)
  mapOutputTracker.trackerEndpoint = RpcUtils.makeDriverRef(MapOutputTracker.ENDPOINT_NAME, conf, rpcEnv)
  logInfo("Registering " + MapOutputTracker.ENDPOINT_NAME)

  val useLegacyMemoryManager = conf.getBoolean("scache.memory.useLegacyMode", false)
  val memoryManager: MemoryManager =
      if (useLegacyMemoryManager) {
        new StaticMemoryManager(conf, numUsableCores)
      } else {
        UnifiedMemoryManager(conf, numUsableCores)
      }

  val blockTransferService = new NettyBlockTransferService(conf, hostname, numUsableCores)


  val blockManagerMasterEndpoint = RpcUtils.makeDriverRef(BlockManagerMaster.DRIVER_ENDPOINT_NAME, conf, rpcEnv)
  val blockManagerMaster = new BlockManagerMaster(blockManagerMasterEndpoint, conf, false)
  var blockManager:BlockManager = null

  logInfo("Client connecting to master " + masterHostname)
  master = RpcUtils.makeDriverRef("Master", conf, rpcEnv)
  clientId = master.askWithRetry[Int](RegisterClient(hostname, port, self))
  blockManager = new BlockManager(clientId.toString, rpcEnv, blockManagerMaster,
    serializerManager, conf, memoryManager, mapOutputTracker, blockTransferService, numUsableCores)
  logInfo(s"Got ID ${clientId} from master")
  blockManager.initialize()
  runTest()

  // rpcEnv.asyncSetupEndpointRefByURI(masterHostname).flatMap { ref =>
  //   master = Some(ref)
  //   ref.ask[Int](RegisterClient(hostname, port, self))
  // }(ThreadUtils.sameThread).onComplete {
  //   case Success(id) =>
  //     clientId = id
  //     blockManager = new BlockManager(clientId.toString, rpcEnv, blockManagerMaster,
  //       serializerManager, conf, memoryManager, mapOutputTracker, blockTransferService, numUsableCores)
  //     logInfo(s"Got ID ${clientId} from master")
  //   case Failure(e) =>
  //     logError("Fail to connect master: " + e.getMessage)
  // }(ThreadUtils.sameThread)
  def runTest(): Unit = {
    val blockIda1 = new ScacheBlockId("scache", 1, 1, 1, 1)
    val blockIda2 = new ScacheBlockId("scache", 1, 1, 1, 2)
    val blockIda3 = new ScacheBlockId("scache", 1, 1, 2, 1)

    // Checking whether master knows about the blocks or not
    assert(blockManagerMaster.getLocations(blockIda1).size > 0, "master was not told about a1")
    assert(blockManagerMaster.getLocations(blockIda2).size > 0, "master was not told about a2")
    assert(blockManagerMaster.getLocations(blockIda3).size == 0, "master was told about a3")

    // Try to fetch remote blocks
    assert(blockManager.getRemoteBytes(blockIda1).size > 0, "fail to get a1")
    assert(blockManager.getRemoteBytes(blockIda2).size > 0, "fail to get a2")

    blockManager.getLocalBytes(blockIda1) match {
      case Some(buffer) =>
        logInfo(s"The size of ${blockIda1} is ${buffer.size}")
      case None =>
        logError(s"Wrong fetch result")
    }
  }

}

object Client extends Logging{
  def main(args: Array[String]): Unit = {
    val conf = new ScacheConf()
    val arguements = new ClientArguments(args, conf)

    conf.set("scache.rpc.askTimeout", "10")
    logInfo("Start Client")
    conf.set("scache.driver.host", arguements.masterIp)
    conf.set("scache.app.id", "test")

    val masterRpcAddress = RpcAddress(arguements.masterIp, arguements.masterPort)

    val rpcEnv = RpcEnv.create("client", arguements.host, arguements.port, conf)
    val clientEndpoint = rpcEnv.setupEndpoint("Client",
      new Client(rpcEnv, arguements.host, RpcEndpointAddress(masterRpcAddress, "Master").toString, arguements.port, conf)
    )
    rpcEnv.awaitTermination()
  }
}
