package org.scache.deploy

import java.io.File
import java.lang.Exception
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption
import java.util.concurrent.ConcurrentHashMap

import org.scache.deploy.DeployMessages._
import org.scache.io.ChunkedByteBuffer
import org.scache.network.netty.NettyBlockTransferService
import org.scache.storage._
import org.scache.storage.memory.{MemoryManager, StaticMemoryManager, UnifiedMemoryManager}
import org.scache.{MapOutputTracker, MapOutputTrackerMaster, MapOutputTrackerWorker}
import org.scache.rpc._
import org.scache.serializer.{JavaSerializer, SerializerManager}
import org.scache.util._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.Exception
import scala.util.{Failure, Random, Success}


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

  override def onStart(): Unit = {
    logInfo("Client connecting to master " + masterHostname)
    master = RpcUtils.makeDriverRef("Master", conf, rpcEnv)
    clientId = master.askWithRetry[Int](RegisterClient(hostname, port, self))
    blockManager = new BlockManager(clientId.toString, rpcEnv, blockManagerMaster,
      serializerManager, conf, memoryManager, mapOutputTracker, blockTransferService, numUsableCores)
    logInfo(s"Got ID ${clientId} from master")
    blockManager.initialize()
  }


  // meta of shuffle tracking
  // val shuffleOutputStatus = new mutable.HashMap[ShuffleKey, ShuffleStatus]()
  // create the future context for client
  private val futureExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("client-future", 128))
  // runTest()

  override def onStop(): Unit = {
    futureExecutionContext.shutdown()
  }


  override def receive: PartialFunction[Any, Unit] = {
    // from deamon
    case MapEnd(appName, jobId, shuffleId, mapId) =>
      mapEnd(appName, jobId, shuffleId, mapId)
    // from master
    case _ =>
      logError("Empty message received !")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case PutBlock(blockId, size) =>
      readBlockFromDaemon(context, blockId, size)
    case RegisterShuffle(appName, jobId, shuffleId, numMapTask, numReduceTask) =>
      context.reply(registerShuffle(appName, jobId, shuffleId, numMapTask, numReduceTask))
    case GetShuffleStatus(appName, jobId, shuffleId) =>
      context.reply(getShuffleStatus(appName, jobId, shuffleId))
    case GetBlock(blockId) =>
      sendBlockToDaemon(context, blockId)
    case _ =>
      logError("Empty message received !")
  }

  def registerShuffle(appName: String, jobId: Int, shuffleId: Int, numMapTask: Int, numReduceTask: Int): Boolean = {
    val res = mapOutputTracker.registerShuffle(appName, jobId, shuffleId, numMapTask, numReduceTask)
    logInfo(s"Trying to register shuffle $appName, $jobId, $shuffleId with map $numMapTask and reduce $numReduceTask, get $res")
    res
  }

  def mapEnd(appName: String, jobId: Int, shuffleId: Int, mapId: Int): Unit = {
    logInfo(s"Map $appName:$jobId:$shuffleId:$mapId finished")
    // master.ask(MapEndToMaster(appName, jobId, shuffleId, mapId))
  }

  // def startMapFetch(blockManagerId: BlockManagerId, appName: String, jobId: Int, shuffleId: Int, mapId: Int): Unit = {
  //   // only pre-fetch remote bytes
  //   if (blockManagerId.executorId.equals(clientId.toString)) {
  //     return
  //   }
  //   logDebug(s"Start to fetch ${appName}_${jobId}_${shuffleId}_${mapId} from ${blockManagerId.host}")
  //   val shuffleKey = ShuffleKey(appName, jobId, shuffleId)
  //   val shuffleStatus = mapOutputTracker.getShuffleStatuses(shuffleKey)
  //   val bIds = new ArrayBuffer[String]()
  //   for (r <- shuffleStatus.reduceArray) {
  //     if (r.host.equals(hostname)) {
  //       // TODO start fetch and add call back to store block in memory
  //       val bId = ScacheBlockId(appName, jobId, shuffleId, mapId, r.id)
  //       bIds.append(bId.toString)
  //     }
  //   }
  //   blockManager.asyncGetRemoteBlock(blockManagerId, bIds.toArray)
  // }

  def readBlockFromDaemon(context: RpcCallContext, blockId: BlockId, size: Int): Unit = {
    if (size == 0) {
      val data = new Array[Byte](0)
      val buf = ByteBuffer.wrap(data)
      val chunkedBuffer = new ChunkedByteBuffer(Array(buf))
      blockManager.putBytes(blockId, chunkedBuffer, StorageLevel.MEMORY_ONLY)
      return
    }
    Future {
      try {
        val f = new File(s"${ScacheConf.scacheLocalDir}/${blockId.toString}")
        val channel = FileChannel.open(f.toPath,
          StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.DELETE_ON_CLOSE)
        val buffer = channel.map(MapMode.READ_WRITE, 0, size)
        // close the channel and delete the tmp file
        val data = new Array[Byte](size)
        buffer.get(data)
        logDebug(s"Get block ${blockId} with $size, hash code: ${data.toSeq.hashCode()}")
        val buf = ByteBuffer.wrap(data)
        val chunkedBuffer = new ChunkedByteBuffer(Array(buf))
        blockManager.putBytes(blockId, chunkedBuffer, StorageLevel.MEMORY_ONLY)
        logDebug(s"Put block $blockId with size $size successfully")
        channel.close()
        context.reply(true)

        // start block transmission immediately
        // val shuffleStatus = getShuffleStatus(blockId)
        // val statuses = mapOutputTracker.getShuffleStatuses(ShuffleKey.fromString(blockId.toString))

      } catch {
        case e: Exception =>
          logError(s"Copy block $blockId error, ${e.getMessage}")
          context.reply(false)
      }

    }(futureExecutionContext)

  }

  def sendBlockToDaemon(context: RpcCallContext, blockId: BlockId): Unit = {
    blockManager.getLocalBytes(blockId) match {
      case Some(buffer) =>
        Future {
          val chunks = buffer.getChunks()
          // it should be a single chunked byte buffer
          assert(chunks.size == 1)
          val bytes = new Array[Byte](chunks(0).remaining())
          chunks(0).get(bytes)
          val f = new File(s"${ScacheConf.scacheLocalDir}/${blockId.toString}")
          val channel = FileChannel.open(f.toPath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
          val writeBuf = channel.map(MapMode.READ_WRITE, 0, bytes.length)
          writeBuf.put(bytes, 0, bytes.length)
          context.reply(bytes.length)
        }(futureExecutionContext)
      case None =>
        // is the block on the air?
        Future {
          if (blockManager.onTheAir(blockId)) {
            Await.result(blockManager.waitForBlock(blockId), Duration.Inf)
            // try again
            blockManager.getLocalBytes(blockId) match {
              case Some(buffer) =>
                val chunks = buffer.getChunks()
                // it should be a single chunked byte buffer
                assert(chunks.size == 1)
                val bytes = new Array[Byte](chunks(0).remaining())
                chunks(0).get(bytes)
                val f = new File(s"${ScacheConf.scacheLocalDir}/${blockId.toString}")
                val channel = FileChannel.open(f.toPath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
                val writeBuf = channel.map(MapMode.READ_WRITE, 0, bytes.length)
                writeBuf.put(bytes, 0, bytes.length)
                context.reply(bytes.length)
              case None =>
                context.reply(-1)
            }
          } else {
            logError(s"Can't find block ${blockId.toString} in local")
            context.reply(-1)
          }
        }(futureExecutionContext)
    }
  }

  private def getShuffleStatus(blockId: BlockId): ShuffleStatus = {
    val shuffleKey = ShuffleKey.fromString(blockId.toString)
    mapOutputTracker.getShuffleStatuses(shuffleKey)
  }

  private def getShuffleStatus(appName: String, shuffleId: Int, jobId: Int): ShuffleStatus = {
    val shuffleKey = ShuffleKey(appName, shuffleId, jobId)
    mapOutputTracker.getShuffleStatuses(shuffleKey)
  }

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

    // shuffle register test
    Thread.sleep(Random.nextInt(1000))
    val res = registerShuffle("scache", 0, 1, 5, 2)
    logInfo(s"TEST: register shuffle got ${res}")
    Thread.sleep(Random.nextInt(1000))
    val statuses = getShuffleStatus(ScacheBlockId("scache", 0, 1, 0, 0))
    for (rs <- statuses.reduceArray) {
      logInfo(s"TEST: shuffle status of ${statuses.shffleId}: reduce ${rs.id} on ${rs.host}")
    }
  }

}

object Client extends Logging{
  def main(args: Array[String]): Unit = {
    val conf = new ScacheConf()
    val arguements = new ClientArguments(args, conf)
    val hostName = Utils.findLocalInetAddress().getHostName
    System.setProperty("SCACHE_DAEMON", s"client-${hostName}")
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
