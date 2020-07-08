/*
* Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.util.UUID

import org.apache.spark.storage.{BlockId, BlockManagerId, TempShuffleBlockId}

/**
 * Opaque object to describe remote state (address, rkey, etc.).
 */
trait Cookie

/**
 * Base class to extend to keep metadata blocks.
 */
case class MetadataBlockId(override val id: UUID) extends TempShuffleBlockId(id) {
  override def name: String = "metadata_" + id
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
  def register(blockId: BlockId, address: Long, size: Long): Cookie

  /**
   * Change location of underlying blockId in memory
   */
  def mutate(blockId: BlockId, newAddress: Long, newSize: Long): Cookie

  /**
   * Indicate that this blockId is not needed any more by an application
   */
  def unregister(blockId: BlockId)

  /**
   * Fetch remote metadata blocks
   */
  def fetch(remoteHost: BlockManagerId, blockId: MetadataBlockId, localAddress: Long, size: Long)

  /**
   * Fetch remote data blocks
   */
  def fetch(remoteHost: BlockManagerId, cookie: Cookie, localAddress: Long, size: Long)
}
