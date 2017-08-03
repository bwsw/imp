package com.bwsw.imp.message.kafka

import java.nio.ByteBuffer

import org.apache.curator.framework.CuratorFramework
import scala.math.BigInt

/**
  * Created by ivan on 03.08.17.
  */
class OffsetKeeper(topic: String)(implicit curatorFramework: CuratorFramework) {
  def createContainers = curatorFramework.createContainers(s"/offsets/$topic")

  def path(partition: Int) = s"/offsets/$topic/$partition"

  def store(offsets: Map[Int, Long]): Unit = {
    createContainers
    offsets foreach {
      case (partition, offset) => curatorFramework
        .create()
        .orSetData()
        .forPath(path(partition), ByteBuffer.allocate(java.lang.Long.SIZE / java.lang.Byte.SIZE).putLong(offset).array())
    }
  }

  def load(partitions: Set[Int]): Map[Int, Long] = {
    createContainers
    partitions.map {
      partition => Option(curatorFramework.checkExists().forPath(path(partition)))
        .fold (-1 -> 0L) {
          _ => partition -> {
            val buffer = ByteBuffer.allocate(java.lang.Long.SIZE / java.lang.Byte.SIZE).put(curatorFramework.getData.forPath(path(partition)))
            buffer.getLong()
          }
        }
    }.filter {
      case (k, v) => k != -1
    }.toMap
  }

}
