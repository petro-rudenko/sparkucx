/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.util.Utils

class UcxShuffleConf(val conf: SparkConf) extends SparkConf {
  private def getUcxConf(name: String) = s"spark.shuffle.ucx.$name"

  object PROTOCOL extends Enumeration {
    val ONE_SIDED, RNDV = Value
  }

  private lazy val PROTOCOL_CONF =
    ConfigBuilder(getUcxConf("protocol"))
      .doc("Which protocol to use: RNDV (default), ONE-SIDED")
      .stringConf
      .checkValue(protocol => protocol == "rndv" || protocol == "one-sided",
        "Invalid protocol. Valid options: rndv / one-sided.")
      .transform(_.toUpperCase.replace("-", "_"))
      .createWithDefault("RNDV")

  private lazy val MEMORY_PINNING =
    ConfigBuilder(getUcxConf("memoryPinning"))
      .doc("Whether to pin whole shuffle data in memory")
      .booleanConf
      .createWithDefault(false)

  lazy val WORKER_ADDRESS_SIZE: ConfigEntry[Long] =
    ConfigBuilder(getUcxConf("maxWorkerSize"))
      .doc("Maximum size of worker address in bytes")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(1024)

  // Memory Pool
  private lazy val PREALLOCATE_BUFFERS =
    ConfigBuilder(getUcxConf("memory.preAllocateBuffers"))
      .doc("Comma separated list of buffer size : buffer count pairs to preallocate in memory pool. E.g. 4k:1000,16k:500")
      .stringConf.createWithDefault("")

  private lazy val WAKEUP_FEATURE =
    ConfigBuilder(getUcxConf("useWakeup"))
      .doc("Whether to use busy polling for workers")
      .booleanConf
      .createWithDefault(false)

  private lazy val USE_SOCKADDR =
    ConfigBuilder(getUcxConf("useSockAddr"))
      .doc("Whether to use socket address to connect executors.")
      .booleanConf
      .createWithDefault(true)

  private lazy val MIN_REGISTRATION_SIZE =
    ConfigBuilder(getUcxConf("memory.minAllocationSize"))
      .doc("Minimal memory registration size in memory pool.")
      .bytesConf(ByteUnit.MiB)
      .createWithDefault(4)

  lazy val minRegistrationSize: Int = conf.getSizeAsBytes(MIN_REGISTRATION_SIZE.key,
    MIN_REGISTRATION_SIZE.defaultValueString).toInt

  private lazy val USE_ODP =
    ConfigBuilder(getUcxConf("useOdp"))
      .doc("Whether to use on demand paging feature, to avoid memory pinning")
      .booleanConf
      .createWithDefault(false)

  lazy val protocol: PROTOCOL.Value = PROTOCOL.withName(
    conf.get(PROTOCOL_CONF.key, PROTOCOL_CONF.defaultValueString))

  lazy val useOdp: Boolean = conf.getBoolean(USE_ODP.key, USE_ODP.defaultValue.get)

  lazy val pinMemory: Boolean = conf.getBoolean(MEMORY_PINNING.key, MEMORY_PINNING.defaultValue.get)

  lazy val maxWorkerAddressSize: Long = conf.getSizeAsBytes(WORKER_ADDRESS_SIZE.key,
    WORKER_ADDRESS_SIZE.defaultValueString)

  lazy val maxMetadataSize: Long = conf.getSizeAsBytes("spark.rapids.shuffle.maxMetadataSize",
    "1024")


  lazy val useWakeup: Boolean = conf.getBoolean(WAKEUP_FEATURE.key, WAKEUP_FEATURE.defaultValue.get)

  lazy val useSockAddr: Boolean = conf.getBoolean(USE_SOCKADDR.key, USE_SOCKADDR.defaultValue.get)

  lazy val preallocateBuffersMap: Map[Long, Int] = {
    conf.get(PREALLOCATE_BUFFERS.key, "").split(",").withFilter(s => s.nonEmpty)
      .map(entry => entry.split(":") match {
        case Array(bufferSize, bufferCount) =>
          (Utils.byteStringAsBytes(bufferSize.trim), bufferCount.toInt)
      }).toMap
  }
}