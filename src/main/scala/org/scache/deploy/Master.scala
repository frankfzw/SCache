package org.scache.deploy

/**
 * Created by frankfzw on 16-8-4.
 */

import java.util.concurrent.ConcurrentHashMap

import org.scache.deploy.DeployMessages.{Heartbeat, MapEndToMaster, RegisterClient}
import org.scache.io.ChunkedByteBuffer
import org.scache.network.netty.NettyBlockTransferService
import org.scache.scheduler.LiveListenerBus
import org.scache.storage.memory.{MemoryManager, StaticMemoryManager, UnifiedMemoryManager}
import org.scache.{MapOutputTracker, MapOutputTrackerMaster, MapOutputTrackerMasterEndpoint}
import org.scache.rpc._
import org.scache.serializer.{JavaSerializer, SerializerManager}
import org.scache.storage._
import org.scache.util._

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

private class Master(
    val rpcEnv: RpcEnv,
    val hostname: String,
    conf: ScacheConf,
    isDriver: Boolean = true,
    isLocal: Boolean) extends ThreadSafeRpcEndpoint with Logging {
  val numUsableCores = conf.getInt("scache.cores", 1)

  val clientIdToInfo: mutable.HashMap[Int, ClientInfo] = new mutable.HashMap[Int, ClientInfo]()
  val hostnameToClientId: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()


  conf.set("scache.master.port", rpcEnv.address.port.toString)

  val serializer = new JavaSerializer(conf)
  val serializerManager = new SerializerManager(serializer, conf)

  val mapOutputTracker = new MapOutputTrackerMaster(conf, isLocal)
  mapOutputTracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
    new MapOutputTrackerMasterEndpoint(rpcEnv, mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))
  // add client list to mapOutputMaster
  mapOutputTracker.hostnameToClientId = hostnameToClientId
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
    new BlockManagerMasterEndpoint(rpcEnv, isLocal, mapOutputTracker, conf))
  val blockManagerMaster = new BlockManagerMaster(blockManagerMasterEndpoint, conf, isDriver)

  // val blockManager = new BlockManager(ScacheConf.DRIVER_IDENTIFIER, rpcEnv, blockManagerMaster,
  //   serializerManager, conf, memoryManager, mapOutputTracker, blockTransferService, numUsableCores)

  // blockManager.initialize()
  private val futureExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("master-future", 128))
  // runTest()

  // meta data to track cluster
  // val shuffleOutputStatus = new ConcurrentHashMap[ShuffleKey, ShuffleStatus]()
  override def onStop(): Unit = {
    futureExecutionContext.shutdown()
  }

  override def receive: PartialFunction[Any, Unit] = {
    case Heartbeat(id, rpcRef) =>
      logInfo(s"Receive heartbeat from ${id}: ${rpcRef}")

    case _ =>
      logError("Empty message received !")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterClient(hostname, port, ref) =>
      context.reply(registerClient(hostname, port, ref))
    // case RegisterShuffleMaster(appName, jobId, shuffleId, numMapTask, numReduceTask) =>
    //   context.reply(registerShuffle(appName, jobId, shuffleId, numMapTask, numReduceTask))
    // case RequestShuffleStatus(shuffleKey) =>
    //   if (shuffleOutputStatus.containsKey(shuffleKey)) {
    //     context.reply(Some(shuffleOutputStatus.get(shuffleKey)))
    //   } else {
    //     context.reply(None)
    //   }
    case MapEndToMaster(appName, jobId, shuffleId, mapId) =>
      logInfo(s"Map task ${appName}_${jobId}_${shuffleId}_${mapId} finished on ${context.senderAddress.host}")
      // startMapFetch(context.senderAddress.host, appName, jobId, shuffleId, mapId)
    case _ =>
      logError("Empty message received !")
  }

  // def registerShuffle(appName: String, jobId: Int, shuffleId: Int, numMapTask: Int, numReduceTask: Int): Boolean = {
  //   val shuffleKey = ShuffleKey(appName, jobId, shuffleId)
  //   if (shuffleOutputStatus.containsKey(shuffleKey)) {
  //     logWarning(s"Shuffle: $shuffleKey has been registered again !")
  //     return false
  //   }
  //   val shuffleStatus = new ShuffleStatus(shuffleId, numMapTask, numReduceTask)

  //   // apply random reduce allocation
  //   val clientList = Random.shuffle(hostnameToClientId.keys.toList)
  //   val numRep = Math.min(conf.getInt("scache.shuffle.replication", 0), clientList.size)
  //   for (i <- 0 until numReduceTask) {
  //     val p = i % clientList.size
  //     val backups = (for (c <- clientList if c != clientList(p)) yield c)
  //     shuffleStatus.reduceArray(i) = new ReduceStatus(i, clientList(p), Random.shuffle(backups).toArray.slice(0, numRep))
  //   }
  //   shuffleOutputStatus.putIfAbsent(shuffleKey, shuffleStatus)
  //   logInfo(s"Register shuffle $appName:$jobId:$shuffleId with map:$numMapTask and reduce:$numReduceTask")

  //   true
  // }
  def registerClient(hostname: String, port: Int, rpcEndpointRef: RpcEndpointRef): Int = {
    if (hostnameToClientId.contains(hostname)) {
      logWarning(s"The client ${hostname}:${hostnameToClientId(hostname)} has been registered again")
      clientIdToInfo.remove(hostnameToClientId(hostname))
    }
    val clientId = Master.CLIENT_ID_GENERATOR.next
    val info = new ClientInfo(clientId, hostname, port, rpcEndpointRef)
    if (hostnameToClientId.contains(hostname)) {
      clientIdToInfo -= hostnameToClientId(hostname)
    }
    hostnameToClientId.update(hostname, clientId)
    clientIdToInfo.update(clientId, info)
    logInfo(s"Register client ${hostname} with id ${clientId} and rpc ref ${rpcEndpointRef}")
    return clientId
  }

  // def startMapFetch(host: String, appName: String, jobId: Int, shuffleId: Int, mapId: Int): Unit = {

  //   val shuffleStatus = mapOutputTracker.getShuffleStatuses(ShuffleKey(appName, jobId, shuffleId))
  //   if (shuffleStatus == null) {
  //     logError(s"Shuffle ${ShuffleKey(appName, jobId, shuffleId).toString()} is not registered")
  //     return
  //   }
  //   Future {
  //     val blockManagerId = hostnameToClientId.get(host) match {
  //       case Some(clientId) =>
  //         blockManagerMaster.getBlockManagerId(clientId.toString).get
  //       case None =>
  //         logError(s"Host $host is not registered")
  //         return
  //     }
  //     for (info <- clientIdToInfo.values) {
  //       logDebug(s"Start notify ${info.host} to fetch $jobId:$shuffleId:$mapId")
  //       info.ref.send(StartMapFetch(blockManagerId, appName, jobId, shuffleId, mapId))
  //     }

  //   }(futureExecutionContext)
  // }


  def runTest(): Unit = {
    val blockIda1 = new ScacheBlockId("scache", 1, 1, 1, 1)
    val blockIda2 = new ScacheBlockId("scache", 1, 1, 1, 2)
    val blockIda3 = new ScacheBlockId("scache", 1, 1, 2, 1)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    val blockManager = new BlockManager(ScacheConf.DRIVER_IDENTIFIER, rpcEnv, blockManagerMaster,
      serializerManager, conf, memoryManager, mapOutputTracker, blockTransferService, numUsableCores)

    blockManager.initialize()

    // Putting a1, a2  and a3 in memory and telling master only about a1 and a2
    blockManager.putSingle(blockIda1, a1, StorageLevel.MEMORY_ONLY)
    blockManager.putSingle(blockIda2, a2, StorageLevel.MEMORY_ONLY)
    blockManager.putSingle(blockIda3, a3, StorageLevel.MEMORY_ONLY, tellMaster = false)

    // Checking whether blocks are in memory
    assert(blockManager.getSingle(blockIda1).isDefined, "a1 was not in blockManager")
    assert(blockManager.getSingle(blockIda2).isDefined, "a2 was not in blockManager")
    assert(blockManager.getSingle(blockIda3).isDefined, "a3 was not in blockManager")

    // Checking whether master knows about the blocks or not
    assert(blockManagerMaster.getLocations(blockIda1).size > 0, "master was not told about a1")
    assert(blockManagerMaster.getLocations(blockIda2).size > 0, "master was not told about a2")
    assert(blockManagerMaster.getLocations(blockIda3).size == 0, "master was told about a3")

    // Drop a1 and a2 from memory; this should be reported back to the master
    assert(blockManager.dropFromMemoryTest(blockIda1, () => Left(a1)) == StorageLevel.DISK_ONLY, "a1 is not drop into disk")
    assert(blockManager.dropFromMemoryTest(blockIda2, () => Left(a2)) == StorageLevel.DISK_ONLY, "a2 is not drop into disk")
    assert(blockManager.getSingle(blockIda1) != None, "a1 is removed from blockManager")
    assert(blockManager.getSingle(blockIda2) != None, "a2 is removed from blockManager")
    assert(blockManagerMaster.getLocations(blockIda1).size != 0, "master removed a1")
    assert(blockManagerMaster.getLocations(blockIda2).size != 0, "master removed a2")

    blockManager.getLocalBytes(blockIda1) match {
      case Some(buffer) =>
        logInfo(s"The size of ${blockIda1} is ${buffer.size}")
      case None =>
        logError(s"Wrong fetch result")
    }
  }


}

object Master extends Logging {
  private val CLIENT_ID_GENERATOR = new IdGenerator

  def main(args: Array[String]): Unit = {
    val hostName = Utils.findLocalInetAddress().getHostName
    System.setProperty("SCACHE_DAEMON", s"master-${hostName}")
    val conf = new ScacheConf()
    val SYSTEM_NAME = "scache.master"
    val arguments = new MasterArguments(args, conf)
    // conf.set("scache.app.id", "test")
    logInfo("Start Master")
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, arguments.host, arguments.port, conf)
    val masterEndpoint = rpcEnv.setupEndpoint("ScacheMaster",
      new Master(rpcEnv, arguments.host, conf, true, arguments.isLocal))
    rpcEnv.awaitTermination()
  //   logInfo(conf.getInt("scache.memory", 1).toString)
  //   logInfo(conf.getString("scache.master", "localhost").toString)
  //   logInfo(conf.getBoolean("scache.boolean", false).toString)
  }
}

private[scache] class ClientInfo(val id: Int, val host: String, val port: Int, val ref: RpcEndpointRef) extends Serializable
