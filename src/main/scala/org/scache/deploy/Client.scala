package org.scache.deploy

import org.scache.deploy.DeployMessages.{RegisterClientSuccess, RegisterClient}
import org.scache.network.netty.NettyBlockTransferService
import org.scache.storage.{BlockManager, BlockManagerMasterEndpoint, BlockManagerMaster}
import org.scache.storage.memory.{UnifiedMemoryManager, StaticMemoryManager, MemoryManager}
import org.scache.{MapOutputTracker, MapOutputTrackerWorker, MapOutputTrackerMaster}
import org.scache.rpc.{ThreadSafeRpcEndpoint, RpcEndpointRef, RpcEnv}
import org.scache.serializer.{SerializerManager, JavaSerializer}
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

  @volatile var master: Option[RpcEndpointRef] = None

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

  val blockManager = new BlockManager(ScacheConf.DRIVER_IDENTIFIER, rpcEnv, blockManagerMaster,
    serializerManager, conf, memoryManager, mapOutputTracker, blockTransferService, numUsableCores)

  override def onStart(): Unit = {
    logInfo("Client connecting to master " + masterHostname)
    rpcEnv.asyncSetupEndpointRefByURI(masterHostname).flatMap { ref =>
      master = Some(ref)
      ref.ask[Int](RegisterClient(hostname, port, self))
    }(ThreadUtils.sameThread).onComplete {
      case Success(id) =>
        clientId = id
        logInfo(s"Got ID ${clientId} from master")
      case Failure(e) =>
        logError("Fail to connect master: " + e.getMessage)
    }(ThreadUtils.sameThread)
  }
}

object Client extends Logging{
  def main(args: Array[String]): Unit = {
    val conf = new ScacheConf()
    val arguements = new ClientArguments(args, conf)

    conf.set("scache.rpc.askTimeout", "10")
    logInfo("Start Client")
    conf.set("scache.driver.host", arguements.masterUrl.toScacheURL)

    val rpcEnv = RpcEnv.create("client", arguements.host, arguements.port, conf)
    val clientEndpoint = rpcEnv.setupEndpoint("Client",
      new Client(rpcEnv, arguements.host, arguements.masterUrl.toScacheURL, arguements.port, conf)
    )
  }
}
