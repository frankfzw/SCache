package org.scache.deploy

/**
 * Created by frankfzw on 16-8-4.
 */

import org.scache.deploy.DeployMessages.{Heartbeat, RegisterClient, RegisterShuffleMaster}
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

  blockManager.initialize()
  // runTest()

  // meta data to track cluster
  val shuffleOutputStatus = new mutable.HashMap[ShuffleKey, ShuffleStatus]()

  override def receive: PartialFunction[Any, Unit] = {
    case Heartbeat(id, rpcRef) =>
      logInfo(s"Receive heartbeat from ${id}: ${rpcRef}")
    case _ =>
      logError("Empty message received !")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterClient(hostname, port, ref) =>
      if (hostnameToClientId.contains(hostname)) {
        logWarning(s"The client ${hostname}:${hostnameToClientId(hostname)} has been registered again")
        clientIdToInfo.remove(hostnameToClientId(hostname))
      }
      val clientId = Master.CLIENT_ID_GENERATOR.next
      val info = new ClientInfo(clientId, hostname, port, ref)
      if (hostnameToClientId.contains(hostname)) {
        clientIdToInfo -= hostnameToClientId(hostname)
      }
      hostnameToClientId.update(hostname, clientId)
      clientIdToInfo.update(clientId, info)
      logInfo(s"Register client ${hostname} with id ${clientId}")
      context.reply(clientId)
    case RegisterShuffleMaster(appName, jobId, shuffleId, numMapTask, numReduceTask) =>
      context.reply(registerShuffle(appName, jobId, shuffleId, numMapTask, numReduceTask))
    case _ =>
      logError("Empty message received !")
  }

  def registerShuffle(appName: String, jobId: Int, shuffleId: Int, numMapTask: Int, numReduceTask: Int): Boolean = {
    val shuffleKey = ShuffleKey(appName, jobId, shuffleId)
    if (shuffleOutputStatus.contains(shuffleKey)) {
      logWarning(s"Shuffle: $shuffleKey has been registered again !")
      return false
    }
    val shuffleStatus = new ShuffleStatus(shuffleId, numMapTask, numReduceTask)

    // apply random reduce allocation
    val clientList = Random.shuffle(hostnameToClientId.keys.toList)
    val numRep = Math.min(conf.getInt("scache.shuffle.replication", 0), clientList.size)
    for (i <- 0 until numReduceTask) {
      val p = i % clientList.size
      val backups = (for (c <- clientList if c != clientList(p)) yield c).toArray
      shuffleStatus.reduceArray(i) = new ReduceStatus(i, clientList(p), Random.shuffle(backups).slice(0, numRep))
    }
    shuffleOutputStatus += (shuffleKey -> shuffleStatus)

    true
  }


  def runTest(): Unit = {
    val blockIda1 = new ScacheBlockId("scache", 1, 1, 1, 1)
    val blockIda2 = new ScacheBlockId("scache", 1, 1, 1, 2)
    val blockIda3 = new ScacheBlockId("scache", 1, 1, 2, 1)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)

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
    logInfo("Start Master")
    val hostName = Utils.findLocalInetAddress().getHostName
    System.setProperty("SCACHE_DAEMON", s"master-${hostName}")
    val conf = new ScacheConf()
    val SYSTEM_NAME = "scache.master"
    val arguments = new MasterArguments(args, conf)
    conf.set("scache.app.id", "test")
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, arguments.host, arguments.port, conf)
    val masterEndpoint = rpcEnv.setupEndpoint("Master",
      new Master(rpcEnv, arguments.host, conf, true, arguments.isLocal))
    rpcEnv.awaitTermination()
  //   logInfo(conf.getInt("scache.memory", 1).toString)
  //   logInfo(conf.getString("scache.master", "localhost").toString)
  //   logInfo(conf.getBoolean("scache.boolean", false).toString)
  }
}

private[deploy] class ClientInfo(val id: Int, val host: String, val port: Int, val ref: RpcEndpointRef) extends Serializable
