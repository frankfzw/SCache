package org.scache.deploy

import org.scache.deploy.DeployMessages.{RegisterClient, RegisterClientSuccess}
import org.scache.network.netty.NettyBlockTransferService
import org.scache.storage.{BlockManager, BlockManagerMaster, BlockManagerMasterEndpoint}
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

}

object Client extends Logging{
  def main(args: Array[String]): Unit = {
    val conf = new ScacheConf()
    val arguements = new ClientArguments(args, conf)

    conf.set("scache.rpc.askTimeout", "10")
    logInfo("Start Client")
    conf.set("scache.driver.host", arguements.masterIp)

    val masterRpcAddress = RpcAddress(arguements.masterIp, arguements.masterPort)

    val rpcEnv = RpcEnv.create("client", arguements.host, arguements.port, conf)
    val clientEndpoint = rpcEnv.setupEndpoint("Client",
      new Client(rpcEnv, arguements.host, RpcEndpointAddress(masterRpcAddress, "Master").toString, arguements.port, conf)
    )
    rpcEnv.awaitTermination()
  }
}
