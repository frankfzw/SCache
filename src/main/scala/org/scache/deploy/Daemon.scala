package org.scache.deploy

import java.io.{ByteArrayOutputStream, File, ObjectOutputStream}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption

import org.scache.deploy.DeployMessages.PutBlock
import org.scache.rpc.{RpcAddress, RpcEndpointRef, RpcEnv}
import org.scache.storage.ScacheBlockId
import org.scache.util._

/**
  * Created by frankfzw on 16-10-31.
  */


class Daemon(platform: String) extends Logging {

  def this() {
    this("test")
  }

  crateEnv(platform)

  def main(args: Array[String]): Unit = {
    val test = new Daemon()
    val block = Array(1, 2, 3, 4)
    val stream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    for (i <- block) {
      oos.writeObject(i)
    }
    oos.close()
    val byteBuf = stream.toByteArray
    test.putBlock("scache", 2, 2, 2, 2, byteBuf)
  }


  def putBlock(appId: String, jobId: Int, shuffleId: Int, mapId: Int, reduceId: Int, data: Array[Byte]): Unit = {
    val blockId = new ScacheBlockId(appId, jobId, shuffleId, mapId, reduceId)
    logDebug(s"Start copying block $blockId with size ${data.size}, hash code: ${data.toSeq.hashCode()}")
    val startTime = System.currentTimeMillis()
    val f = new File(s"${ScacheConf.scacheLocalDir}/${blockId.toString}")
    val channel = FileChannel.open(f.toPath, StandardOpenOption.READ, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
    val buf = channel.map(MapMode.READ_WRITE, 0, data.size)
    buf.put(data, 0, data.size)
    logDebug(s"Writing block $blockId to buffer with size ${data.size}")
    Daemon.clientRef.send(PutBlock(blockId, data.size))
    val endTime = System.currentTimeMillis()
    logDebug(s"Copy block $blockId to SCache in ${endTime - startTime} ms")
  }

  private[scache] def crateEnv(platform: String): Unit = {
    if (Daemon.clientRef == null) {
      var t: Thread = null
      Daemon.initLock.synchronized {
        if (Daemon.clientRef == null) {
          t = new Thread {
            override def run() = {
              Daemon.conf = new ScacheConf()
              logInfo("Start Daemon")
              val localIP = Utils.findLocalInetAddress().getHostAddress
              val rpcEnv = RpcEnv.create(s"${platform}_deameon", localIP, 12345, Daemon.conf)

              val clientRpcAddress = RpcAddress(localIP, 5678)
              Daemon.clientRef = rpcEnv.setupEndpointRef(clientRpcAddress, "Client")
              rpcEnv.awaitTermination()
            }
          }
        }
      }
      if (t != null) {
        Daemon.tid = t.getId
        t.start()
      }
    }
  }

}

private[scache] object Daemon extends Logging{
  val initLock = new Object
  @volatile var clientRef: RpcEndpointRef = null
  var conf: ScacheConf = null
  var tid: Long = -1
}
