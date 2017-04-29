package org.scache.deploy

import java.io.{ByteArrayOutputStream, File, ObjectOutputStream, RandomAccessFile}
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

  System.setProperty("SCACHE_DAEMON", s"daemon-${Utils.findLocalInetAddress().getHostName}")
  private val asyncThreadPool =
    ThreadUtils.newDaemonCachedThreadPool("scache-daemon-async-thread-pool")
  private implicit val asyncExecutionContext = ExecutionContext.fromExecutorService(asyncThreadPool)

  _logDir = scacheHome
  private val conf = new ScacheConf(scacheHome)
  private val clientPort = conf.getInt("scache.client.port", 5678)
  private val daemonPort = conf.getInt("scache.daemon.port", 12345)
  private val host = Utils.findLocalInetAddress().getHostAddress
  private val rpcEnv = RpcEnv.create("scache.daemon", host, daemonPort, conf, true)
  private val clientRef = rpcEnv.setupEndpointRef(RpcAddress(host, clientPort), "ScacheClient")

  // start daemon rpc thread
  doAsync[Unit]("Start Scache Daemon") {
    logInfo("Start deamon")
    rpcEnv.awaitTermination()
  }

  def putBlock(blockId: String, data: Array[Byte], rawLen: Int, compressedLen: Int): Unit = {
    val scacheBlockId = BlockId.apply(blockId)
    if (!scacheBlockId.isInstanceOf[ScacheBlockId]) {
      logError(s"Unexpected block type, except ScacheBlockId, got ${scacheBlockId.getClass.getSimpleName}")
    }
    logDebug(s"Start copying block $blockId with size $rawLen")
    doAsync[Unit](s"Copy block $blockId") {
      val startTime = System.currentTimeMillis()
      val f = new RandomAccessFile(s"${ScacheConf.scacheLocalDir}/$blockId", "rw")
      val channel = f.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, data.length)
      channel.put(data)
      val res = clientRef.askWithRetry[Boolean](PutBlock(scacheBlockId, data.length))
      if (res) {
        logDebug(s"Copy block $blockId succeeded in ${System.currentTimeMillis() - startTime} ms")
      } else {
        logDebug(s"Copy block $blockId failed")
      }
    }
  }
  def getBlock(blockId: String): Option[Array[Byte]] = {
    val scacheBlockId = BlockId.apply(blockId)
    if (!scacheBlockId.isInstanceOf[ScacheBlockId]) {
      logError(s"Unexpected block type, except ScacheBlockId, got ${scacheBlockId.getClass.getSimpleName}")
      return None
    }
    val startTime = System.currentTimeMillis()
    val size = clientRef.askWithRetry[Int](GetBlock(scacheBlockId))
    if (size < 0) {
      return None
    }
    val f = new RandomAccessFile(s"${ScacheConf.scacheLocalDir}/$blockId", "rw")
    val channel = f.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, size)
    val arrayBuf = new Array[Byte](size)
    channel.get(arrayBuf)
    f.close()
    logDebug(s"Get block $blockId succeeded in ${System.currentTimeMillis() - startTime} ms")
    Some(arrayBuf)
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

  def mapStart(jobId: Int, shuffleId: Int, mapId: Int): Unit = {
    doAsync[Unit] ("Map Start") {
      clientRef.send(MapStart(platform, jobId, shuffleId, mapId))
    }
  }

  def mapEnd(jobId: Int, shuffleId: Int, mapId: Int, sizes: Array[Long]): Unit = {
    doAsync[Unit] ("Map End") {
      clientRef.send(MapEnd(platform, jobId, shuffleId, mapId, sizes))
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

object Daemon {
  def main(args: Array[String]): Unit = {
    val daemon = new Daemon("/home/spark/SCache", "test")
    daemon.putBlock(ScacheBlockId("test", 0, 0, 0, 0).toString, Array[Byte](5), 5, 5)
    Thread.sleep(10000)
  }
}

