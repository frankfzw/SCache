package org.scache.deploy

import org.scache.rpc.RpcEndpointRef

/**
 * Created by frankfzw on 16-9-13.
 */
private[deploy] trait DeployMessage extends Serializable

private[deploy] object DeployMessages {
  case class Heartbeat(id: String, worker: RpcEndpointRef) extends DeployMessage
  case class RegisterClient(hostname: String, port: Int, worker: RpcEndpointRef)

  //master to client

  sealed trait RegisterClientResponse

  case class RegisterClientSuccess(clientId: Int) extends DeployMessage with RegisterClientResponse
}
