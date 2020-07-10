/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.io.Closeable

import scala.collection.mutable

import org.openucx.jucx.{UcxCallback, UcxException}
import org.openucx.jucx.ucp.{UcpEndpoint, UcpEndpointParams, UcpRequest, UcpWorker}
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockManagerId


/**
 * Worker per thread wrapper, that maintains connection and progress logic.
 */
class UcxWorkerWrapper(val worker: UcpWorker, val transport: UcxShuffleTransportImpl)
  extends Closeable with Logging {

  private final val connections = mutable.Map.empty[BlockManagerId, UcpEndpoint]

  override def close(): Unit = {
    connections.foreach{
      case (_, endpoint) => endpoint.close()
    }
    connections.clear()
    worker.close()
  }

  /**
   * The only place for worker progress
   */
  private[ucx] def progress(): Int = {
    worker.progress()
  }

  /**
   * Establish connections to known instances.
   */
  def preconnect() {
    transport.getPeersAddresses.keys.foreach(getConnection)
  }

  def getConnection(blockManagerId: BlockManagerId): UcpEndpoint = {
    val workerAdresses = transport.getPeersAddresses

    if (!workerAdresses.contains(blockManagerId)) {
      // Block untill there's no worker address for this BlockManagerID
      val startTime = System.currentTimeMillis()
      val timeout = SparkEnv.get.conf.getTimeAsMs("spark.network.timeout", "100")
      workerAdresses.synchronized {
        while (workerAdresses.get(blockManagerId) == null) {
          workerAdresses.wait(timeout)
          if (System.currentTimeMillis() - startTime > timeout) {
            throw new UcxException(s"Didn't get worker address for $blockManagerId during $timeout")
          }
        }
      }
    }

    connections.getOrElseUpdate(blockManagerId, {
      logInfo(s"Worker from thread ${Thread.currentThread().getName} connecting to $blockManagerId")
      val endpointParams = new UcpEndpointParams()
        .setUcpAddress(workerAdresses(blockManagerId))
     worker.newEndpoint(endpointParams)
    })
  }

  def fetchBlocksByCookies(remoteHost: BlockManagerId, cookies: Seq[Cookie], resultBuffer: UcxMemoryBlock,
                           cb: OperationCallback): Unit = {
    logInfo(s"Fetching blocks by ${cookies.mkString(",")}")
    val ep = getConnection(remoteHost)
    var offset = 0L
    for (cookie <- cookies) {
      val ucxCookie = cookie.asInstanceOf[UcxCookie]
      val rkey = ep.unpackRemoteKey(ucxCookie.rkey)
      ep.getNonBlockingImplicit(ucxCookie.address, rkey, resultBuffer.address + offset, ucxCookie.size)
      offset += ucxCookie.size
    }
    ep.flushNonBlocking(new UcxCallback() {
      override def onSuccess(request: UcpRequest): Unit = {
        cb.onSuccess()
      }
    })
  }

}

