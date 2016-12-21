package org.scache.deploy

import org.scache.rpc.RpcEndpointRef
import org.scache.storage.BlockId
import org.scache.util.ShuffleKey

/**
 * Created by frankfzw on 16-9-13.
 */
private[deploy] trait DeployMessage extends Serializable

private[deploy] object DeployMessages {

  sealed trait ToDeployMaster
  case class Heartbeat(id: String, worker: RpcEndpointRef) extends ToDeployMaster
  case class RegisterClient(hostname: String, port: Int, worker: RpcEndpointRef) extends ToDeployMaster
  case class PutBlockMaster(scacheBlockId: BlockId, size: Int) extends ToDeployMaster
  case class RegisterShuffleMaster(appName:String, jobId: Int, shuffleId: Int, numMapTask: Int, numReduceTask: Int) extends ToDeployMaster
  case class RequestShuffleStatus(shuffleKey: ShuffleKey) extends ToDeployMaster

  sealed trait FromDaemon
  case class PutBlock(scacheBlockId: BlockId, size: Int) extends FromDaemon
  case class GetBlock(scacheBlockId: BlockId) extends FromDaemon
  case class RegisterShuffle(appName: String, jobId: Int, shuffleId: Int, numMapTask: Int, numReduceTask: Int) extends FromDaemon

  sealed trait  ToDeployClient
  case class RegisterClientSuccess(clientId: Int) extends ToDeployClient
}
