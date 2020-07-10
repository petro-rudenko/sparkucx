package org.apache.spark.shuffle.ucx.rpc

import scala.collection.immutable.HashMap
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{ExecutorAdded, IntroduceAllExecutors}
import org.apache.spark.shuffle.ucx.utils.SerializableDirectBuffer
import org.apache.spark.storage.BlockManagerId

class UcxDriverRpcEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint with Logging {

  private val endpoints: mutable.Set[RpcEndpointRef] = mutable.HashSet.empty
  private var blockManagerToWorkerAddress:
    Map[(BlockManagerId, RpcEndpointRef), SerializableDirectBuffer] = HashMap.empty

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case message@ExecutorAdded(blockManagerId: BlockManagerId, endpoint: RpcEndpointRef,
    ucxWorkerAddress: SerializableDirectBuffer) => {
      // Driver receives a message from executor with it's workerAddress
      // 1. Introduce existing members of a cluster
      logInfo(s"Received $message")
      if (blockManagerToWorkerAddress.nonEmpty) {
        val msg = IntroduceAllExecutors(blockManagerToWorkerAddress.keys.toList,
          blockManagerToWorkerAddress.values.toList)
        logInfo(s"replying $msg to $blockManagerId")
        context.reply(msg)
      }
      blockManagerToWorkerAddress += (blockManagerId, endpoint) -> ucxWorkerAddress
      // 2. For each existing member introduce newly joined executor.
      endpoints.foreach(ep => {
        logInfo(s"Sending $message to $ep")
        ep.send(message)
      })
      logInfo(s"Connecting back to address: ${context.senderAddress}")
      endpoints.add(endpoint)
    }
  }
}
