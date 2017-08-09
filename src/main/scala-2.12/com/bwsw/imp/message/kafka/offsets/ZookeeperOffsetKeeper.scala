package com.bwsw.imp.message.kafka.offsets

import java.nio.ByteBuffer

import org.apache.curator.framework.CuratorFramework

/**
  * Created by Ivan Kudryavtsev on 03.08.17.
  */
class ZookeeperOffsetKeeper(implicit curatorFramework: CuratorFramework) extends OffsetKeeper {
  private def createContainers(topic: String) = curatorFramework.createContainers(s"/offsets/$topic")
  private def path(topic: String, partition: Int) = s"/offsets/$topic/$partition"
  private def allocateBuffer() = ByteBuffer.allocate(java.lang.Long.SIZE / java.lang.Byte.SIZE)


  def store(topic: String, offsets: Map[Int, Long]): Unit = {
    createContainers(topic)
    offsets foreach {
      case (partition, offset) => curatorFramework
        .create()
        .orSetData()
        .forPath(path(topic, partition), allocateBuffer().putLong(offset).array())
    }
  }

  def load(topic: String, partitions: Set[Int]): Map[Int, Long] = {
    createContainers(topic)
    partitions.map {
      partition => Option(curatorFramework.checkExists().forPath(path(topic, partition)))
        .fold (partition -> 0L) {
          _ => partition -> {
            val buffer = allocateBuffer()
            buffer.put(curatorFramework.getData.forPath(path(topic, partition)))
            buffer.rewind()
            buffer.getLong()
          }
        }
    }.filter {
      case (k, v) => k != -1
    }.toMap
  }

}
