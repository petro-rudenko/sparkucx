package org.apache.spark.shuffle.ucx.perf

import java.net.InetSocketAddress
import java.nio.ByteBuffer

import org.apache.commons.cli.{GnuParser, HelpFormatter, Options}
import org.openucx.jucx.UcxUtils
import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.shuffle.ucx.{UcxBlockId, UcxMemoryBlock, UcxShuffleTransportImpl}
import org.apache.spark.shuffle.ucx.rpc.UcxDriverRpcEndpoint
import org.apache.spark.util.{RpcUtils, Utils}

object UcxShufflePerfTool {

  case class PerfOptions(remoteAddress: InetSocketAddress, numBlocks: Int, blockSize: Long)

  case class TestBlockId(id: Int) extends UcxBlockId

  private val HELP_OPTION = "h"
  private val ADDRESS_OPTION = "a"
  private val NUMBER_OPTION = "n"
  private val SIZE_OPTION = "s"

  private val DRIVER_ENDPOINT = "ucx-driver"

  private def initOptions(): Options = {
    val options = new Options()
    options.addOption(HELP_OPTION, "help", false,"display help message")
    options.addOption(ADDRESS_OPTION, "address", true, "address of remote host")
    options.addOption(NUMBER_OPTION, "num-blocks", true, "number of blocks to transfer")
    options.addOption(SIZE_OPTION, "block-size", true, "size of block to transfer")
  }

  private def parseOptions(args: Array[String]): PerfOptions = {
    val parser = new GnuParser()
    val options = initOptions()
    val cmd = parser.parse(options, args)
    if (cmd.hasOption(HELP_OPTION)) {
      new HelpFormatter().printHelp("UcxShufflePerfTool", options)
      System.exit(0)
    }
    val inetAddresss = if (cmd.hasOption(ADDRESS_OPTION)) {
      val Array(host, port) = cmd.getOptionValue(ADDRESS_OPTION).split(":")
      new InetSocketAddress(host, Integer.parseInt(port))
    } else {
      null
    }
    PerfOptions(inetAddresss,
      Integer.parseInt(cmd.getOptionValue(NUMBER_OPTION, "5")),
      Utils.byteStringAsBytes(cmd.getOptionValue(SIZE_OPTION, "4m")))
  }

  def main(args: Array[String]): Unit = {
    val params = parseOptions(args)
    val conf = new SparkConf()
    val transport = new UcxShuffleTransportImpl

    transport.init()

    val rpcEnv = if (params.remoteAddress == null) {
      val res = RpcEnv.create("ucx-rpc-env", "0.0.0.0", "0.0.0.0",
        12345, conf, new SecurityManager(conf), 1, clientMode=false)
      val driverEndpoint = new UcxDriverRpcEndpoint(rpcEnv)
      res.setupEndpoint(, driverEndpoint)
      println(s" Starting server at ${res.address}...")

      val blocksIds = (0 to params.numBlocks).map(x => TestBlockId(x))
      val blocks = (0 to params.numBlocks).map(_ => {
        val b = ByteBuffer.allocateDirect(params.blockSize.toInt)
        UcxMemoryBlock(UcxUtils.getAddress(b), params.blockSize)
      })
      transport.register(blocksIds, blocks)

      res
    } else {
      RpcEnv.create("ucx-rpc-env", "0.0.0.0", "0.0.0.0",
        12345, conf, new SecurityManager(conf), 1, clientMode=true)
      val driverEndpoint = RpcUtils.makeDriverRef(driverEndpointName, conf, rpcEnv)
    }

    rpcEnv.shutdown()
  }

}
