/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.scache

import java.io._
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, ThreadPoolExecutor}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.scache.deploy.{ReduceStatus, ShuffleStatus}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map}
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import org.scache.util.Logging
import org.scache.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.scache.scheduler.{CacheId, CacheStatistics, MapStatus}
import org.scache.storage.{BlockId, BlockManagerId, ScacheBlockId, ShuffleBlockId}
import org.scache.util._

import scala.util.Random

private[scache] sealed trait MapOutputTrackerMessage
private[scache] case object StopMapOutputTracker extends MapOutputTrackerMessage
private[scache] case class RegisterShuffleMaster(appName:String, jobId: Int, shuffleId: Int, numMapTask: Int, numReduceTask: Int)
  extends MapOutputTrackerMessage
private[scache] case class RequestShuffleStatus(shuffleKey: ShuffleKey)
  extends MapOutputTrackerMessage
private[scache] case class UpdateMapBlockSize(blockId: BlockId, size: Long)
  extends MapOutputTrackerMessage

private[scache] case class GetMapOutputMessage(shuffleId: Int, context: RpcCallContext)

/** RpcEndpoint class for MapOutputTrackerMaster */
private[scache] class MapOutputTrackerMasterEndpoint(
    override val rpcEnv: RpcEnv, tracker: MapOutputTrackerMaster, conf: ScacheConf)
  extends RpcEndpoint with Logging {

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RequestShuffleStatus(shuffleKey) =>
      val status = tracker.getShuffleStatuses(shuffleKey)
      if (status == null) {
        context.reply(None)
      } else {
        context.reply(Some(status))
      }
    case UpdateMapBlockSize(blockId, size) =>
      context.reply(tracker.updateMapBlockSize(blockId, size))
    case RegisterShuffleMaster(appName:String, jobId: Int, shuffleId: Int, numMapTask: Int, numReduceTask: Int) =>
      context.reply(tracker.registerShuffle(appName, jobId, shuffleId, numMapTask, numReduceTask))
    case StopMapOutputTracker =>
      logInfo("MapOutputTrackerMasterEndpoint stopped!")
      context.reply(true)
      stop()
  }
}

/**
 * Class that keeps track of the location of the map output of
 * a stage. This is abstract because different versions of MapOutputTracker
 * (driver and executor) use different HashMap to store its metadata.
 */
private[scache] abstract class MapOutputTracker(conf: ScacheConf) extends Logging {

  /** Set to the MapOutputTrackerMasterEndpoint living on the driver. */
  var trackerEndpoint: RpcEndpointRef = _


  protected val shuffleOutputStatus: Map[ShuffleKey, ShuffleStatus]



  /**
   * Send a message to the trackerEndpoint and get its result within a default timeout, or
   * throw a ScacheException if this fails.
   */
  protected def askTracker[T: ClassTag](message: Any): T = {
    try {
      trackerEndpoint.askWithRetry[T](message)
    } catch {
      case e: Exception =>
        logError("Error communicating with MapOutputTracker", e)
        throw new Exception("Error communicating with MapOutputTracker", e)
    }
  }

  /** Send a one-way message to the trackerEndpoint, to which we expect it to reply with true. */
  protected def sendTracker(message: Any) {
    val response = askTracker[Boolean](message)
    if (response != true) {
      throw new Exception(
        "Error reply received from MapOutputTracker. Expecting true, got " + response.toString)
    }
  }

  def getShuffleStatuses(shuffleKey: ShuffleKey): ShuffleStatus = {
    shuffleOutputStatus.get(shuffleKey) match {
      case Some(status) =>
        return status
      case None =>
        shuffleOutputStatus.synchronized {
          if (!shuffleOutputStatus.contains(shuffleKey)) {
            val shuffleStatus = askTracker[Option[ShuffleStatus]](RequestShuffleStatus(shuffleKey))
            shuffleStatus match {
              case Some(status) =>
                shuffleOutputStatus += (shuffleKey -> status)
                logInfo(s"Get shuffle output status from master: " +
                  s"ID:${status.shffleId}, map task number: ${status.mapTaskNum}, reduce task number: ${status.reduceTaskNum}, details: ${status.reduceArray}")
                return status
              case None =>
                logError(s"Shuffle is unregistered: ${shuffleKey.toString()}")
                throw new Exception(s"Shuffle is unregistered: ${shuffleKey.toString()}")
            }
          } else {
            return shuffleOutputStatus.get(shuffleKey).get
          }
        }
    }
  }


  def registerShuffle(appName: String, jobId: Int, shuffleId: Int, numMapTask: Int, numReduceTask: Int): Boolean = {
    val res = askTracker[Boolean](RegisterShuffleMaster(appName, jobId, shuffleId, numMapTask, numReduceTask))
    res
  }

  def updateMapBlockSize(blockId: BlockId, size: Long): Boolean = {
    blockId match {
      case ScacheBlockId(appName, jobId, shuffleId, mapId, reduceId) =>
        if (size > 0) {
          return askTracker[Boolean](UpdateMapBlockSize(blockId, size))
        } else {
          return true
        }
      case _ =>
        logError(s"Got wrong block type, require ${ScacheBlockId.getClass.toString}, got ${blockId.getClass.toString}")
        return false
    }
  }



  /** Stop the tracker. */
  def stop() { }
}

/**
 * MapOutputTracker for the driver.
 */
private[scache] class MapOutputTrackerMaster(conf: ScacheConf, isLocal: Boolean)
  extends MapOutputTracker(conf) {

  protected val shuffleOutputStatus = new ConcurrentHashMap[ShuffleKey, ShuffleStatus]().asScala

  var hostnameToClientId: scala.collection.mutable.HashMap[String, Int] = _

  /** Cache a serialized version of the output statuses for each shuffle to send them out faster */

  /** A poison endpoint that indicates MessageLoop should exit its message loop. */

  // Exposed for testing
  override def registerShuffle(appName: String, jobId: Int, shuffleId: Int, numMapTask: Int, numReduceTask: Int): Boolean = {
    val shuffleKey = ShuffleKey(appName, jobId, shuffleId)
    if (shuffleOutputStatus.contains(shuffleKey)) {
      logWarning(s"Shuffle: $shuffleKey has been registered again !")
      return false
    }
    val shuffleStatus = new ShuffleStatus(shuffleId, numMapTask, numReduceTask)

    // apply random reduce allocation
    val clientList = Random.shuffle(hostnameToClientId.keys.toList)
    logDebug(s"Alive client list: ${clientList.size}, $clientList")
    val numRep = Math.min(conf.getInt("scache.shuffle.replication", 0), clientList.size)
    for (i <- 0 until numReduceTask) {
      val p = i % clientList.size
      val backups = (for (c <- clientList if c != clientList(p)) yield c)
      shuffleStatus.reduceArray(i) = new ReduceStatus(i, clientList(p), Random.shuffle(backups).toArray.slice(0, numRep), numMapTask)
    }
    shuffleOutputStatus.putIfAbsent(shuffleKey, shuffleStatus)
    logInfo(s"Register shuffle $appName:$jobId:$shuffleId with map:$numMapTask and reduce:$numReduceTask")

    true
  }

  override def getShuffleStatuses(shuffleKey: ShuffleKey): ShuffleStatus = {
    shuffleOutputStatus.get(shuffleKey) match {
      case Some(status) => status
      case None => null
    }
  }

  override def updateMapBlockSize(blockId: BlockId, size: Long): Boolean = {
    val shuffleKey = ShuffleKey.fromString(blockId.toString)
    val bId = blockId.asInstanceOf[ScacheBlockId]
    shuffleOutputStatus.get(shuffleKey) match {
      case Some(status) =>
        if (bId.mapId >= status.mapTaskNum || bId.reduceId >= status.reduceTaskNum) {
          logError(s"Wrong block id, got ${bId.toString}, excepted map task number is ${status.mapTaskNum}, reduce task number is ${status.reduceTaskNum}")
          return false
        }
        status.reduceArray(bId.reduceId).size(bId.mapId) = size
        logDebug(s"Update shuffle ${shuffleKey} map block with Id: ${blockId.toString} and size: $size")
        return true
      case None =>
        logError(s"Shuffle ${shuffleKey} is not registered")
        return false
    }
  }




  override def stop() {
    sendTracker(StopMapOutputTracker)
    shuffleOutputStatus.clear()
    trackerEndpoint = null
  }
}

/**
 * MapOutputTracker for the executors, which fetches map output information from the driver's
 * MapOutputTrackerMaster.
 */
private[scache] class MapOutputTrackerWorker(conf: ScacheConf) extends MapOutputTracker(conf) {
  protected val shuffleOutputStatus = new scala.collection.mutable.HashMap[ShuffleKey, ShuffleStatus]()
}

private[scache] object MapOutputTracker extends Logging {

  val ENDPOINT_NAME = "MapOutputTracker"
  private val DIRECT = 0
  private val BROADCAST = 1

  // Serialize an array of map output locations into an efficient byte format so that we can send
  // it to reduce tasks. We do this by compressing the serialized bytes using GZIP. They will
  // generally be pretty compressible because many map outputs will be on the same hostname.
  def serializeMapStatuses(statuses: Array[MapStatus], isLocal: Boolean): Array[Byte] = {
    val out = new ByteArrayOutputStream
    out.write(DIRECT)
    val objOut = new ObjectOutputStream(new GZIPOutputStream(out))
    Utils.tryWithSafeFinally {
      // Since statuses can be modified in parallel, sync on it
      statuses.synchronized {
        objOut.writeObject(statuses)
      }
    } {
      objOut.close()
    }
    val arr = out.toByteArray
    arr
  }

  // Opposite of serializeMapStatuses.
  def deserializeMapStatuses(bytes: Array[Byte]): Array[MapStatus] = {
    assert (bytes.length > 0)

    def deserializeObject(arr: Array[Byte], off: Int, len: Int): AnyRef = {
      val objIn = new ObjectInputStream(new GZIPInputStream(
        new ByteArrayInputStream(arr, off, len)))
      Utils.tryWithSafeFinally {
        objIn.readObject()
      } {
        objIn.close()
      }
    }

    bytes(0) match {
      case DIRECT =>
        deserializeObject(bytes, 1, bytes.length - 1).asInstanceOf[Array[MapStatus]]
      case _ => throw new IllegalArgumentException("Unexpected byte tag = " + bytes(0))
    }
  }

  /**
   * Given an array of map statuses and a range of map output partitions, returns a sequence that,
   * for each block manager ID, lists the shuffle block IDs and corresponding shuffle block sizes
   * stored at that block manager.
   *
   * If any of the statuses is null (indicating a missing location due to a failed mapper),
   * throws a FetchFailedException.
   *
   * @param shuffleId Identifier for the shuffle
   * @param startPartition Start of map output partition ID range (included in range)
   * @param endPartition End of map output partition ID range (excluded from range)
   * @param statuses List of map statuses, indexed by map ID.
   * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
   *         and the second item is a sequence of (shuffle block ID, shuffle block size) tuples
   *         describing the shuffle blocks that are stored at that block manager.
   */
  private def convertMapStatuses(
      shuffleId: Int,
      startPartition: Int,
      endPartition: Int,
      statuses: Array[MapStatus]): Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    assert (statuses != null)
    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(BlockId, Long)]]
    for ((status, mapId) <- statuses.zipWithIndex) {
      if (status == null) {
        val errorMessage = s"Missing an output location for shuffle $shuffleId"
        logError(errorMessage)
        throw new Exception(s"$shuffleId, $startPartition, $errorMessage")
      } else {
        for (part <- startPartition until endPartition) {
          splitsByAddress.getOrElseUpdate(status.location, ArrayBuffer()) +=
            ((ShuffleBlockId(shuffleId, mapId, part), status.getSizeForBlock(part)))
        }
      }
    }

    splitsByAddress.toSeq
  }
}
