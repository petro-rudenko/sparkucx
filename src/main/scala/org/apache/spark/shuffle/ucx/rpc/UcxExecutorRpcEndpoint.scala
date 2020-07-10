package org.apache.spark.shuffle.ucx.rpc

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{BlocksResolved, ExecutorAdded, IntroduceAllExecutors, ResolveBlocks}
import org.apache.spark.shuffle.ucx.utils.SerializableDirectBuffer
import org.apache.spark.shuffle.ucx.{UcxBlockId, UcxShuffleTransport, UcxShuffleTransportImpl}
import org.apache.spark.storage.BlockManagerId

class UcxExecutorRpcEndpoint(override val rpcEnv: RpcEnv, transport: UcxShuffleTransport)
  extends RpcEndpoint  with Logging {

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case ResolveBlocks(blockIds: Seq[UcxBlockId]) =>
      logTrace(s"Receive request ResolveBlocks${blockIds}")
      context.reply(BlocksResolved(
        transport.asInstanceOf[UcxShuffleTransportImpl].resolveBlocksByBlockIds(blockIds)))
  }

  override def receive: PartialFunction[Any, Unit] = {
    case ExecutorAdded(blockManagerId: BlockManagerId,
                       endpoint: RpcEndpointRef,
                       ucxWorkerAddress: SerializableDirectBuffer) => {
      logInfo(s"Received ExecutorAdded($blockManagerId , $ucxWorkerAddress, ${endpoint.name} $endpoint)")
      UcxExecutorRpcEndpoint.blockManagerIdToRpcEndpoint.put(blockManagerId,
        rpcEnv.setupEndpointRef(endpoint.address, endpoint.name))
      transport.asInstanceOf[UcxShuffleTransportImpl].addExecutor(blockManagerId, ucxWorkerAddress.value)
    }
    case IntroduceAllExecutors(blockManagerIds: Seq[(BlockManagerId, RpcEndpointRef)],
      ucxWorkerAddresses: Seq[SerializableDirectBuffer]) => {
      logInfo(s"Received IntroduceAllExecutors($blockManagerIds)")
      for (i <- blockManagerIds.indices) {
        val (blockManagerId, endpoint) = blockManagerIds(i)
        UcxExecutorRpcEndpoint.blockManagerIdToRpcEndpoint.put(blockManagerId,
          rpcEnv.setupEndpointRef(endpoint.address, endpoint.name))
        transport.asInstanceOf[UcxShuffleTransportImpl]
          .addExecutor(blockManagerIds(i)._1, ucxWorkerAddresses(i).value)
      }
    }
  }
}

object UcxExecutorRpcEndpoint {
  val blockManagerIdToRpcEndpoint:
    ConcurrentMap[BlockManagerId, RpcEndpointRef] = new ConcurrentHashMap[BlockManagerId, RpcEndpointRef]()
}
