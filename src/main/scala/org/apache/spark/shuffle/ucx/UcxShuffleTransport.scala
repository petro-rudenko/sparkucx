/*
* Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import org.apache.spark.storage.BlockManagerId

/**
 * Opaque object to describe remote state (address, rkey, etc.).
 */
trait Cookie

/**
 * Base class to extend to keep metadata blocks.
 */
case class UcxBlockId(id: Long)


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
  def register(blockId: UcxBlockId, address: Long, size: Long): Cookie

  /**
   * Change location of underlying blockId in memory
   */
  def mutate(blockId: UcxBlockId, newAddress: Long, newSize: Long): Cookie

  /**
   * Indicate that this blockId is not needed any more by an application
   */
  def unregister(blockId: UcxBlockId): Unit

  /**
   * Fetch remote metadata block
   */
  def fetchMetadata(remoteHost: BlockManagerId, blockId: UcxBlockId, localAddress: Long, size: Long)

  def fetchMetadata(remoteHost: BlockManagerId, blockIds: Seq[UcxBlockId], localAddress: Long,
                    size: Long)


  /**
   * Fetch remote data blocks
   */
  def fetchBlock(remoteHost: BlockManagerId, cookie: Cookie, localAddress: Long, size: Long)
}
