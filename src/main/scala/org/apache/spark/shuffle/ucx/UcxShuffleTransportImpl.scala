package org.apache.spark.shuffle.ucx
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global

import org.openucx.jucx.UcxUtils
import org.openucx.jucx.ucp._
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.rpc.UcxExecutorRpcEndpoint
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{BlocksResolved, ResolveBlocks}
import org.apache.spark.shuffle.ucx.utils.SerializableDirectBuffer
import org.apache.spark.storage.BlockManagerId

private[ucx] case class UcxCookie(address: Long, size: Long, rkey: ByteBuffer) extends Cookie {
  override def toByteBuffer: ByteBuffer = {
    rkey.rewind()
    val result = ByteBuffer.allocateDirect(8+8+4+rkey.capacity())
    result.putLong(address)
    result.putLong(size)
    result.putInt(rkey.capacity())
    result.put(rkey)
    rkey.rewind()
    result.rewind()
    result
  }
}

object UcxCookie {
  def fromByteBuffer(buffer: ByteBuffer): UcxCookie = {
    val address = buffer.getLong
    val size = buffer.getLong
    val rkeySize = buffer.getInt
    val rkeyBuffer = buffer.slice()
    buffer.position(buffer.position() + rkeySize)
    UcxCookie(address, size, rkeyBuffer)
  }
}

class UcxShuffleTransportImpl extends UcxShuffleTransport with Logging{

  // UCX entities
  private var ucxContext: UcpContext = _
  private var globalWorker: UcpWorker = _
  private val ucpWorkerParams = new UcpWorkerParams()

  private var threadLocalWorker: ThreadLocal[UcxWorkerWrapper] = _

  private val memBlockToUcpMemory: ConcurrentHashMap[UcxMemoryBlock, UcpMemory] =
    new ConcurrentHashMap[UcxMemoryBlock, UcpMemory]
  private val blockManagerToWorkerAddress: mutable.Map[BlockManagerId, ByteBuffer] = mutable.HashMap.empty
  private val registeredBlocks: ConcurrentHashMap[UcxBlockId, UcxMemoryBlock] =
    new ConcurrentHashMap[UcxBlockId, UcxMemoryBlock]

  private val allocatedWorkers = ConcurrentHashMap.newKeySet[UcxWorkerWrapper]

  /**
   * Initialize transport resources
   */
  override def init(): Unit = {
    ucxContext = new UcpContext(new UcpParams().requestTagFeature().requestRmaFeature())
    globalWorker = ucxContext.newWorker(ucpWorkerParams)

    threadLocalWorker = ThreadLocal.withInitial(() => {
      val localWorker = ucxContext.newWorker(ucpWorkerParams)
      val result = new UcxWorkerWrapper(localWorker, this)
      allocatedWorkers.add(result)
      result
    })

  }

  private[ucx] def getUcpContext: UcpContext = ucxContext

  private[ucx] def addExecutor(blockManagerId: BlockManagerId, workerAddress: ByteBuffer): Unit = {
    if (blockManagerId != SparkEnv.get.blockManager.blockManagerId) {
      blockManagerToWorkerAddress.synchronized{
        blockManagerToWorkerAddress.put(blockManagerId, workerAddress)
        blockManagerToWorkerAddress.notifyAll()
      }
    }
  }

  def getPeersAddresses: mutable.Map[BlockManagerId, ByteBuffer] = blockManagerToWorkerAddress

  private[ucx] def getGlobalWorkerAddress: SerializableDirectBuffer =
    new SerializableDirectBuffer(globalWorker.getAddress)

  /**
   * Close all transport resources
   */
  override def close(): Unit = {
    allocatedWorkers.forEach(_.close())
    globalWorker.close()
    ucxContext.close()
  }

  private def memoryMapInternal(memoryBlock: UcxMemoryBlock): ByteBuffer = {
    if (!memoryBlock.isInstanceOf[MetadataUcxMemoryBlock]) {
      val memMapParams = new UcpMemMapParams().setAddress(memoryBlock.address).setLength(memoryBlock.size)
      val ucpMemory = ucxContext.memoryMap(memMapParams)
      memBlockToUcpMemory.put(memoryBlock, ucpMemory)
      ucpMemory.getRemoteKeyBuffer
    } else {
      null
    }
  }

  /**
   * Registers blockId for either metadata blocks, or data blocks on SERVER side.
   */
  override def register(blocks: Seq[UcxBlockId], memoryBlocks: Seq[UcxMemoryBlock]): Seq[Cookie] = {
    val result: Array[Cookie] = new Array[Cookie](blocks.size)
    for (i <- blocks.indices) {
      val rkey = memoryMapInternal(memoryBlocks(i))
      blocks(i).memoryBlock = memoryBlocks(i)
      memoryBlocks(i).refCount.incrementAndGet()
      registeredBlocks.put(blocks(i), memoryBlocks(i))
      result(i) = UcxCookie(memoryBlocks(i).address, memoryBlocks(i).size, rkey)
    }
    result
  }


  /**
   * Register single underlying memory to multiple blocks, with corresponding lengths.
   */
  override def register(blockIds: Seq[UcxBlockId],
                        block: UcxMemoryBlock, lengths: Seq[Long]): Seq[Cookie] = {
    require(blockIds.length == lengths.length, "BlockIds and lengths must be of equal size")
    val rkey = memoryMapInternal(block)
    blockIds.foreach(b => b.memoryBlock = block)
    block.refCount = new AtomicInteger(lengths.size)
    var offset = 0L
    val result: Array[Cookie] = new Array[Cookie](blockIds.size)
    for (i <- lengths.indices) {
      val blockBuff = UcxUtils.getByteBufferView(block.address + offset, lengths(i).toInt)
      result(i) = UcxCookie(block.address + offset, lengths(i), rkey)
      registeredBlocks.put(blockIds(i), block)
      offset += lengths(i)
    }
    result
  }

  /**
   * Change location of underlying blockId in memory
   */
  override def mutate(block: UcxBlockId, memoryBlock: UcxMemoryBlock,
                      callback: OperationCallback): Unit = {
    // For now just unregister blocks and re-register again
    unregister(Array(block))
    register(Array(block), Array(memoryBlock))
    callback.onSuccess()
  }

  /**
   * Indicate that this blockId is not needed any more by an application
   */
  override def unregister(blockIds: Seq[UcxBlockId]): Unit = {
    blockIds.foreach(block => {
      val memoryBlock = registeredBlocks.get(block)
      if (memoryBlock != null && memoryBlock.refCount.decrementAndGet() == 0) {
        memBlockToUcpMemory.asScala.remove(memoryBlock).foreach(_.deregister())
      }
    })
  }

  /**
   * Fetch remote metadata block
   */
  override def fetchBlocksByBlockIds(remoteHost: BlockManagerId, blockIds: Seq[UcxBlockId],
                                     resultMemory: UcxMemoryBlock, cb: OperationCallback): Unit = {
    val rpcEndpointRef = UcxExecutorRpcEndpoint.blockManagerIdToRpcEndpoint.get(remoteHost)
    if (rpcEndpointRef == null) {
      logWarning(s"No ep for blockManager($remoteHost):" +
        s"keys: ${UcxExecutorRpcEndpoint.blockManagerIdToRpcEndpoint.keySet().asScala.mkString(",")}")
    }

    rpcEndpointRef.ask[BlocksResolved](ResolveBlocks(blockIds)).foreach{
      case BlocksResolved(blocks: SerializableDirectBuffer) =>
        // TODO: recv directly to result buffer
        val resultBuffer = UcxUtils.getByteBufferView(resultMemory.address, resultMemory.size.toInt)
        resultBuffer.put(blocks.buffer)
        cb.onSuccess()
    }
  }

  private[ucx] def resolveBlocksByBlockIds(blockIds: Seq[UcxBlockId]): SerializableDirectBuffer = {
    val memBlocks = blockIds.map(b => registeredBlocks.get(b))
    val totalSize = memBlocks.map(_.size).sum
    val result = ByteBuffer.allocateDirect(totalSize.toInt)

    memBlocks.foreach(block => {
      val buffer = block.asInstanceOf[MetadataUcxMemoryBlock].buffer
      buffer.rewind()
      result.put(buffer)
      buffer.rewind()
      result
    })

    new SerializableDirectBuffer(result)
  }

  /**
   * Fetch remote data blocks
   */
  override def fetchBlocksByCookies(remoteHost: BlockManagerId, cookies: Seq[Cookie],
                                    resultBuffer: UcxMemoryBlock, cb: OperationCallback): Unit = {
    threadLocalWorker.get().fetchBlocksByCookies(remoteHost, cookies, resultBuffer, cb)
  }

  /**
   * Progress outstanding operations. This routine is blocking. It's important to call this routine
   * within same thread that submitted {@code fetchBlocksByCookies} request.
   */
  override def progress(): Unit = threadLocalWorker.get().progress()

}
