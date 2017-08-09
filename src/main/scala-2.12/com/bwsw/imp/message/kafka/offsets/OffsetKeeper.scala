package com.bwsw.imp.message.kafka.offsets

/**
  * Created by Ivan Kudryavtsev on 08.08.17.
  */
trait OffsetKeeper {
  def store(topic: String, offsets: Map[Int, Long]): Unit
  def load(topic: String, partitions: Set[Int]): Map[Int, Long]
}
