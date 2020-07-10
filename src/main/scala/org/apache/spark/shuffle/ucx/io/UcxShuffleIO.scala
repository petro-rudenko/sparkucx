package org.apache.spark.shuffle.ucx.io

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.api.{ShuffleDriverComponents, ShuffleExecutorComponents}
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleDataIO
import org.apache.spark.shuffle.ucx.UcxShuffleManager

class UcxShuffleIO(sparkConf: SparkConf) extends LocalDiskShuffleDataIO(sparkConf) with Logging {

  override def driver(): ShuffleDriverComponents = {
    SparkEnv.get.shuffleManager.asInstanceOf[UcxShuffleManager].initTransport()
    super.driver()
  }

  override def executor(): ShuffleExecutorComponents = {
    new UcxShuffleExecutorComponents(sparkConf)
  }
}
