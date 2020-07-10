package org.apache.spark.shuffle.ucx

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

import org.openucx.jucx.UcxUtils
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, BlockStoreClient, DownloadFileManager}
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}

class UcxShuffleClient(transport: UcxShuffleTransport,
                       blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])])
  extends BlockStoreClient with Logging {

  private val blockIdToCookie = mutable.HashMap.empty[String, Cookie]

  resolveCookiesByBlockIds(blocksByAddress)

  // TODO: resolve only remote blocks.
  private def resolveCookiesByBlockIds(blocksByAddress:
                                       Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])]): Unit = {
    val currentBlockManager = SparkEnv.get.blockManager.blockManagerId
    blocksByAddress.foreach {
      case (blockManagerId, blocks) => {
        // TODO: check host local enabled?
        if (blockManagerId.host != currentBlockManager.host) {

          val ucxConf = SparkEnv.get.shuffleManager.asInstanceOf[UcxShuffleManager].ucxShuffleConf
          val resultBuffer = ByteBuffer.allocateDirect(blocks.size * (20L + ucxConf.rkeySize).toInt)
          val resultMemoryBlock = UcxMemoryBlock(UcxUtils.getAddress(resultBuffer), resultBuffer.capacity())
          val blockIds = blocks.map(b => {
            val shuffleBlock = b._1.asInstanceOf[ShuffleBlockId]
            UcxMetadataBlockId(shuffleBlock.shuffleId, shuffleBlock.mapId, shuffleBlock.reduceId)
          })

          transport.fetchBlocksByBlockIds(blockManagerId,
            blockIds, resultMemoryBlock, new OperationCallback {
              private val startTime = System.currentTimeMillis()

              override def onSuccess(): Unit = {
                logInfo(s"Resolved ${blockIds.size} metadata blocks in " +
                  s"${System.currentTimeMillis() - startTime} ms.")
                blockIds.foreach(blockId => {
                  val cookie =  UcxCookie.fromByteBuffer(resultBuffer)
                  require(cookie.asInstanceOf[UcxCookie].address > 0, "Cookie corrupted")
                  blockIdToCookie.synchronized {
                    blockIdToCookie.put(blockId.name, cookie)
                    blockIdToCookie.notifyAll()
                  }
                })
              }

              override def onFailure(throwable: Throwable): Unit = ???
            })
        }
      }
    }
  }

  override def fetchBlocks(host: String, port: Int, execId: String,
                           blockIds: Array[String], listener: BlockFetchingListener,
                           downloadFileManager: DownloadFileManager): Unit = {
    val cookies: Array[Cookie] = blockIds.map(blockId => {
      if (!blockIdToCookie.contains(blockId)) {
        blockIdToCookie.synchronized {
          while (!blockIdToCookie.contains(blockId)) {
            logWarning(s"No cookie for $blockId on $execId. Waiting to be resolved")
            blockIdToCookie.wait()
          }
        }
      }
      blockIdToCookie(blockId)
    })
    val totalSize = cookies.map(_.asInstanceOf[UcxCookie].size).sum
    val blockManagerId = BlockManagerId(execId, host, port, Option.empty[String])
    val memPool = SparkEnv.get.shuffleManager.asInstanceOf[UcxShuffleManager].memoryPool
    val resultMemory = memPool.get(totalSize.toInt)
    val resultMemoryBlock = UcxMemoryBlock(resultMemory.getAddress, resultMemory.getLength)
    val refCount = new AtomicInteger(blockIds.length)
    transport.fetchBlocksByCookies(blockManagerId, cookies, resultMemoryBlock, new OperationCallback {
      private val startTime = System.currentTimeMillis()

      override def onSuccess(): Unit = {
        logInfo(s" Fetched ${blockIds.length} blocks of size ${totalSize} " +
          s"in ${System.currentTimeMillis() - startTime}")
        val resultBuffer = resultMemory.getBuffer
        resultBuffer.rewind()
        for (i <- blockIds.indices) {
          val block = blockIds(i)
          val blockSize = cookies(i).asInstanceOf[UcxCookie].size.toInt
          val buffer = resultBuffer.slice()
          buffer.limit(blockSize)
          resultBuffer.position(resultBuffer.position() + blockSize)
          listener.onBlockFetchSuccess(block,
            new NioManagedBuffer(buffer) {
              override def release: ManagedBuffer = {
                if (refCount.decrementAndGet == 0) {
                  memPool.put(resultMemory)
                }
                this
              }
            })
        }
      }

      override def onFailure(throwable: Throwable): Unit = ???
    })
  }

  override def close(): Unit = {
    blockIdToCookie.clear()
  }
}
