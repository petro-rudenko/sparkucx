package org.apache.spark.shuffle.ucx.io

import java.util
import java.util.Optional

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.api.{ShuffleMapOutputWriter, SingleSpillShuffleMapOutputWriter}
import org.apache.spark.shuffle.sort.io.{LocalDiskShuffleExecutorComponents, LocalDiskShuffleMapOutputWriter, LocalDiskSingleSpillMapOutputWriter}
import org.apache.spark.shuffle.ucx.rpc.UcxExecutorRpcEndpoint
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.ExecutorAdded
import org.apache.spark.shuffle.ucx.{UcxShuffleBlockResolver, UcxShuffleManager, UcxShuffleTransport, UcxShuffleTransportImpl}
import org.apache.spark.util.RpcUtils
import org.apache.spark.{SparkConf, SparkEnv}


class UcxShuffleExecutorComponents(sparkConf: SparkConf)
  extends LocalDiskShuffleExecutorComponents(sparkConf) with Logging {

  var ucxShuffleTransport: UcxShuffleTransport = _
  private var blockResolver: UcxShuffleBlockResolver = _

  override def initializeExecutor(appId: String, execId: String, extraConfigs: util.Map[String, String]): Unit = {
    val ucxShuffleManager = SparkEnv.get.shuffleManager.asInstanceOf[UcxShuffleManager]
    ucxShuffleManager.initTransport()
    blockResolver = ucxShuffleManager.shuffleBlockResolver
  }

  override def createMapOutputWriter(shuffleId: Int, mapTaskId: Long, numPartitions: Int): ShuffleMapOutputWriter = {
    if (blockResolver == null) {
      throw new IllegalStateException(
        "Executor components must be initialized before getting writers.")
    }
    new LocalDiskShuffleMapOutputWriter(
      shuffleId, mapTaskId, numPartitions, blockResolver, sparkConf)
  }

  override def createSingleFileMapOutputWriter(shuffleId: Int, mapId: Long):
  Optional[SingleSpillShuffleMapOutputWriter] = {
    if (blockResolver == null) {
      throw new IllegalStateException(
        "Executor components must be initialized before getting writers.")
    }
    Optional.of(new LocalDiskSingleSpillMapOutputWriter(shuffleId, mapId, blockResolver))
  }

}
