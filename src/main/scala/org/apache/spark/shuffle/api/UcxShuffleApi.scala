package org.apache.spark.shuffle.api
import java.io.Closeable

import org.apache.spark.storage.{BlockId, BlockManagerId}

trait Cookie

trait UcxShuffleApi extends Closeable {
  def init()
  def close()

  def register(blockId: BlockId, address: Long, size: Long): Cookie
  def mutate(blockId: BlockId, newAddress: Long, newSize: Long): Cookie
  def unregister(blockId: BlockId)

  def fetch(remoteHost: BlockManagerId, blockId: BlockId, localAddress: Long, size: Long)
  def fetch(remoteHost: BlockManagerId, cookie: Cookie, localAddress: Long, size: Long)
}
