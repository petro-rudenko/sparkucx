package org.apache.spark.shuffle.ucx.rpc

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.shuffle.ucx.UcxBlockId
import org.apache.spark.shuffle.ucx.utils.SerializableDirectBuffer
import org.apache.spark.storage.BlockManagerId

object UcxRpcMessages {

  /**
   * Called from executor to driver, to introduce ucx worker address.
   */
  case class ExecutorAdded(blockManagerId: BlockManagerId,
                           endpoint: RpcEndpointRef,
                           ucxWorkerAddress: SerializableDirectBuffer)

  /**
   * Reply from driver with all executors in the cluster with their worker addresses.
   */
  case class IntroduceAllExecutors(blockManagerIds: Seq[(BlockManagerId, RpcEndpointRef)],
                                   ucxWorkerAddresses: Seq[SerializableDirectBuffer])

  /**
   * Request from executor to another to send blocks metadata.
   */
  case class ResolveBlocks(blockIds: Seq[UcxBlockId])

  /**
   * Reply from executor with needed metadata for the block.
   */
  case class BlocksResolved(resolvedBlocks: SerializableDirectBuffer)
}
