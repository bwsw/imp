package com.bwsw.imp.message.kafka

import java.nio.ByteBuffer

import org.apache.curator.framework.CuratorFramework

/**
  * Created by ivan on 03.08.17.
  */
class OffsetKeeper(topic: String)(implicit curatorFramework: CuratorFramework) {
  private def createContainers() = curatorFramework.createContainers(s"/offsets/$topic")
  private def path(partition: Int) = s"/offsets/$topic/$partition"
  private def allocateBuffer() = ByteBuffer.allocate(java.lang.Long.SIZE / java.lang.Byte.SIZE)


  def store(offsets: Map[Int, Long]): Unit = {
    createContainers()
    offsets foreach {
      case (partition, offset) => curatorFramework
        .create()
        .orSetData()
        .forPath(path(partition), allocateBuffer().putLong(offset).array())
    }
  }

  def load(partitions: Set[Int]): Map[Int, Long] = {
    createContainers()
    partitions.map {
      partition => Option(curatorFramework.checkExists().forPath(path(partition)))
        .fold (partition -> 0L) {
          _ => partition -> {
            val buffer = allocateBuffer()
            buffer.put(curatorFramework.getData.forPath(path(partition)))
            buffer.rewind()
            buffer.getLong()
          }
        }
    }.filter {
      case (k, v) => k != -1
    }.toMap
  }

}
