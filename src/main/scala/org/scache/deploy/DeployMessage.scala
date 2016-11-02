package org.scache.deploy

import org.scache.rpc.RpcEndpointRef
import org.scache.storage.BlockId

/**
 * Created by frankfzw on 16-9-13.
 */
private[deploy] trait DeployMessage extends Serializable

private[deploy] object DeployMessages {
  case class Heartbeat(id: String, worker: RpcEndpointRef) extends DeployMessage
  case class RegisterClient(hostname: String, port: Int, worker: RpcEndpointRef) extends DeployMessage

  //master to client
  case class PutBlock(scacheBlockId: BlockId, size: Int) extends DeployMessage
  case class GetBlock(scacheBlockId: BlockId) extends DeployMessage

  sealed trait RegisterClientResponse

  case class RegisterClientSuccess(clientId: Int) extends DeployMessage with RegisterClientResponse
}
