/*
* Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import org.openucx.jucx.UcxUtils
import org.apache.spark.storage.BlockManagerId

/**
 * Opaque object to describe remote memory block (address, rkey, etc.).
 */
trait Cookie extends Serializable {
  def toByteBuffer: ByteBuffer
}

case class UcxMemoryBlock(address: Long, size: Long) {
  // Number of blockIds referenced to it.
  private[ucx] var refCount: AtomicInteger = new AtomicInteger(0)
}

/**
 * To keep reference to byte buffer, to prevent it from GC, and to indicate that this
 * block would be received by fetchBlocksByBlockIds
 */
class MetadataUcxMemoryBlock(val buffer: ByteBuffer)
  extends UcxMemoryBlock(UcxUtils.getAddress(buffer), buffer.capacity())

/**
 * Base class to extend to keep metadata blocks. Important to extend this block as a case class.
 */
abstract class UcxBlockId {
  // Pointer to registered memory block. MemoryBlock may have multiple blockIds pointing to it.
  private[ucx] var memoryBlock: UcxMemoryBlock = _
}

trait OperationCallback {
  def onSuccess()
  def onFailure(throwable: Throwable)
}

trait UcxShuffleTransport {

  /**
   * Initialize transport resources
   */
  def init()

  /**
   * Close all transport resources
   */
  def close()

  /**
   * Registers blockId for either metadata blocks, or data blocks on SERVER side.
   */
  def register(blockIds: Seq[UcxBlockId], blocks: Seq[UcxMemoryBlock]): Seq[Cookie]

  /**
   * Register single underlying memory to multiple blocks, with corresponding lengths.
   */
  def register(blockIds: Seq[UcxBlockId], block: UcxMemoryBlock, lengths: Seq[Long]): Seq[Cookie]

  /**
   * Change location of underlying blockId in memory
   */
  def mutate(blockIds: UcxBlockId, block: UcxMemoryBlock, callback: OperationCallback)

  /**
   * Indicate that this blockId is not needed any more by an application
   */
  def unregister(blockIds: Seq[UcxBlockId])

  // def prefetchBlocksByBlockIds(remoteHost: BlockManagerId, blockIds: Seq[UcxBlockId])
  /**
   * Fetch remote metadata block
   */
  def fetchBlocksByBlockIds(remoteHost: BlockManagerId, blockIds: Seq[UcxBlockId],
                            resultBuffer: UcxMemoryBlock, cb: OperationCallback)

  /**
   * Fetch remote data blocks
   */
  def fetchBlocksByCookies(remoteHost: BlockManagerId, cookies: Seq[Cookie], resultBuffer: UcxMemoryBlock,
                           cb: OperationCallback)

  /**
   * Progress outstanding operations. This routine is blocking. It's important to call this routine
   * within same thread that submitted {@code fetchBlocksByCookies} request.
   */
  def progress()
}
