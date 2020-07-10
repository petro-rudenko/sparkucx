package org.apache.spark.shuffle.ucx

import java.io.File
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.openucx.jucx.UcxException
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, UcxShuffleConf}
import org.apache.spark.storage.ShuffleBlockId


case class UcxShuffleBlockId(shuffleId: Int, mapId: Long, reduceId: Int) extends UcxBlockId {

  def this(shuffleBlockId: ShuffleBlockId) = {
    this(shuffleBlockId.shuffleId, shuffleBlockId.mapId, shuffleBlockId.reduceId)
  }

  def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId
}

case class UcxMetadataBlockId(shuffleId: Int, mapId: Long, reduceId: Int) extends UcxBlockId {
  def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId
}

class UcxShuffleBlockResolver(conf: UcxShuffleConf, transport: UcxShuffleTransport)
  extends IndexShuffleBlockResolver(conf) {

  type MapId = Long

  private val numPartitionsForMapId = new ConcurrentHashMap[MapId, Int]

  override def writeIndexFileAndCommit(shuffleId: ShuffleId, mapId: Long,
                                       lengths: Array[Long], dataTmp: File): Unit = {
    super.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)
    val dataFile = getDataFile(shuffleId, mapId)
    val fileChannel = FileChannel.open(dataFile.toPath, StandardOpenOption.READ, StandardOpenOption.WRITE)
    val baseAddress = UnsafeUtils.mmap(fileChannel, 0L, dataFile.length())
    val blocks = lengths.indices.map(reduceId => UcxShuffleBlockId(shuffleId, mapId, reduceId))

    val cookies = transport.register(blocks, UcxMemoryBlock(baseAddress, dataFile.length()), lengths)

    val metadataBlocks = blocks.map(blockId =>
      UcxMetadataBlockId(blockId.shuffleId, blockId.mapId, blockId.reduceId))

    transport.register(metadataBlocks, cookies.map((cookie: Cookie) => {
        val resultBuffer = cookie.toByteBuffer
        if (resultBuffer.capacity() > conf.rkeySize + 20) {
          throw new UcxException(s"rkeySize ${resultBuffer.capacity() - 20} is bigger than configured" +
            s"${conf.RKEY_SIZE.key}=${conf.rkeySize}")
        }
        new MetadataUcxMemoryBlock(resultBuffer)
      }
    ))

    numPartitionsForMapId.put(mapId, lengths.length)
    UnsafeUtils.munmap(baseAddress, dataFile.length())
    fileChannel.close()
  }

  override def removeDataByMap(shuffleId: ShuffleId, mapId: Long): Unit = {
    val numRegisteredBlocks = numPartitionsForMapId.get(mapId)
    val blocks = (0 to numRegisteredBlocks)
      .map(reduceId => UcxShuffleBlockId(shuffleId, mapId, reduceId))
    transport.unregister(blocks)
  }

  override def stop(): Unit = {
    numPartitionsForMapId.keys.asScala.foreach(mapId => removeDataByMap(0, mapId))
  }

}
