package org.apache.spark.shuffle.ucx

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success

import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.sort.SortShuffleManager.canUseBatchFetch
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.ucx.memory.UcxMemoryPool
import org.apache.spark.shuffle.ucx.rpc.{UcxDriverRpcEndpoint, UcxExecutorRpcEndpoint}
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{ExecutorAdded, IntroduceAllExecutors}
import org.apache.spark.util.{RpcUtils, Utils}
import org.apache.spark.{SecurityManager, SparkConf, SparkEnv, TaskContext}


class UcxShuffleManager(conf: SparkConf, isDriver: Boolean) extends SortShuffleManager(conf) {

  val ucxShuffleTransport = new UcxShuffleTransportImpl()
  val ucxShuffleConf = new UcxShuffleConf(conf)
  @volatile private var initialized: Boolean = false

  val memoryPool = new UcxMemoryPool(ucxShuffleTransport.getUcpContext, ucxShuffleConf)

  private val driverEndpointName = "ucx-shuffle-driver"

  logInfo("Starting UcxShuffleManager")

  def initTransport(): Unit = this.synchronized {
    if (!initialized) {
      if (isDriver) {
        val rpcEnv = SparkEnv.get.rpcEnv
        val driverEndpoint = new UcxDriverRpcEndpoint(rpcEnv)
        rpcEnv.setupEndpoint(driverEndpointName, driverEndpoint)
      } else {
        val blockManager = SparkEnv.get.blockManager.blockManagerId
        val rpcEnv = RpcEnv.create("ucx-rpc-env", blockManager.host, blockManager.host,
          blockManager.port, conf, new SecurityManager(conf), 1, clientMode=false)
        logDebug("Initializing ucx transport")
        ucxShuffleTransport.init()
        val executorEndpoint = new UcxExecutorRpcEndpoint(rpcEnv, ucxShuffleTransport)
        val blockManagerId = SparkEnv.get.blockManager.blockManagerId
        val endpoint = rpcEnv.setupEndpoint(
          UcxShuffleManager.executorEndpointName + s"-${blockManagerId.executorId}",
          executorEndpoint)

        val driverEndpoint = RpcUtils.makeDriverRef(driverEndpointName, conf, rpcEnv)
        driverEndpoint.ask[IntroduceAllExecutors](ExecutorAdded(SparkEnv.get.blockManager.blockManagerId,
          endpoint,
          ucxShuffleTransport.asInstanceOf[UcxShuffleTransportImpl].getGlobalWorkerAddress))
          .andThen{
            case Success(msg) => {
              logInfo(s"Receive reply $msg")
              executorEndpoint.receive(msg)
            }
          }
      }
      initialized = true
    }
  }

  override val shuffleBlockResolver = new UcxShuffleBlockResolver(ucxShuffleConf, ucxShuffleTransport)

  override def getReader[K, C](
                                handle: ShuffleHandle,
                                startPartition: Int,
                                endPartition: Int,
                                context: TaskContext,
                                metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    val blocksByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
      handle.shuffleId, startPartition, endPartition)
    new UcxShuffleReader(ucxShuffleTransport,
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], blocksByAddress, context, metrics,
      shouldBatchFetch = canUseBatchFetch(startPartition, endPartition, context))
  }

  override def getReaderForRange[K, C]( handle: ShuffleHandle,
                                        startMapIndex: Int,
                                        endMapIndex: Int,
                                        startPartition: Int,
                                        endPartition: Int,
                                        context: TaskContext,
                                        metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    val blocksByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByRange(
      handle.shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)
    new UcxShuffleReader(ucxShuffleTransport,
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], blocksByAddress, context, metrics,
      shouldBatchFetch = canUseBatchFetch(startPartition, endPartition, context))
  }

}

object UcxShuffleManager {
  val executorEndpointName = "ucx-shuffle-executor"
}
