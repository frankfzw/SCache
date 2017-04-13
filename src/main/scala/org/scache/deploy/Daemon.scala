package org.scache.deploy

import java.io.{ByteArrayOutputStream, File, ObjectOutputStream}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption

import org.scache.deploy.DeployMessages._
import org.scache.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.scache.storage.{BlockId, ScacheBlockId}
import org.scache.util._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by frankfzw on 16-10-31.
  */


class Daemon(
  scacheHome: String,
  platform: String) extends Logging {

  private val asyncThreadPool =
    ThreadUtils.newDaemonCachedThreadPool("scache-daemon-async-thread-pool")
  private implicit val asyncExecutionContext = ExecutionContext.fromExecutorService(asyncThreadPool)

  private val conf = new ScacheConf(scacheHome)
  private val clientPort = conf.getInt("scache.client.port", 5678)
  private val daemonPort = conf.getInt("scache.daemon.port", 12345)
  private val host = Utils.findLocalInetAddress().getHostAddress
  private val rpcEnv = RpcEnv.create("scache.daemon", host, daemonPort, conf)
  private val clientRef = rpcEnv.setupEndpointRef(RpcAddress(host, clientPort), "ScacheClient")

  // start daemon rpc thread
  doAsync[Unit]("Start Scache Daemon") {
    rpcEnv.awaitTermination()
  }

  def putBlock(blockId: String, data: Array[Byte], rawLen: Int, compressedLen: Int): Unit = {
    val scacheBlockId = BlockId.apply(blockId)
    logDebug(s"Start copying block $blockId with size $rawLen")
    doAsync[Unit](s"Copy block $blockId") {
      val f = new File(s"${ScacheConf.scacheLocalDir}/$blockId")
      val channel = FileChannel.open(f.toPath, StandardOpenOption.READ,
        StandardOpenOption.WRITE, StandardOpenOption.CREATE)
      val buf = channel.map(FileChannel.MapMode.READ_WRITE, 0, data.length)
      buf.put(data)
      val res = clientRef.askWithRetry[Boolean](PutBlock(scacheBlockId, data.length))
      if (res) {
        logDebug(s"Copy block $blockId succeeded")
      } else {
        logDebug(s"Copy block $blockId failed")
      }
    }
  }
  def getBlock(blockId: String): Array[Byte] = {
    val scacheBlockId = BlockId.apply(blockId)
    val size = clientRef.askWithRetry[Int](GetBlock(scacheBlockId))
    val f = new File(s"${ScacheConf.scacheLocalDir}/$blockId")
    val channel = FileChannel.open(f.toPath, StandardOpenOption.READ,
      StandardOpenOption.WRITE, StandardOpenOption.CREATE)
    val buf = channel.map(FileChannel.MapMode.READ_WRITE, 0, size)
    val arrayBuf = new Array[Byte](size)
    buf.get(arrayBuf)
    arrayBuf
  }
  def registerShuffles(jobId: Int, shuffleIds: Array[Int], maps: Array[Int], reduces: Array[Int]): Unit = {
    doAsync[Unit] ("Register Shuffles") {
      val res = clientRef.askWithRetry[Boolean](RegisterShuffle(platform, jobId, shuffleIds, maps, reduces))
      if (res) {
        logInfo(s"Register shuffles ${shuffleIds} succeeded")
      } else {
        logInfo(s"Register shuffles ${shuffleIds} failed")
      }
    }
  }
  def mapEnd(jobId: Int, shuffleId: Int, mapId: Int): Unit = {
    doAsync[Unit] ("Map End") {
      clientRef.send(MapEnd(platform, jobId, shuffleId, mapId))
    }
  }
  def getShuffleStatus(jobId: Int, shuffleId: Int): mutable.HashMap[Int, Array[String]] = {
    val statuses = clientRef.askWithRetry[ShuffleStatus](GetShuffleStatus(platform, jobId, shuffleId))
    val ret = new mutable.HashMap[Int, Array[String]]
    for (rs <- statuses.reduceArray) {
      val hosts = Array(rs.host) ++ rs.backups
      ret += (rs.id -> hosts)
    }
    ret
  }

  def stop(): Unit = {
    asyncThreadPool.shutdownNow()
  }

  private def doAsync[T](actionMessage: String)(body: => T) {
    val future = Future {
      logDebug(actionMessage)
      body
    }
    future.onSuccess { case response =>
      logDebug("Done " + actionMessage + ", response is " + response)
    }
    future.onFailure { case t: Throwable =>
      logError("Error in " + actionMessage, t)
    }
  }
}

