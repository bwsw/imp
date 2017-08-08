package com.bwsw.imp.message.kafka

import com.bwsw.imp.common.zookeeper.ZookeeperOffsetKeeper
import com.bwsw.imp.curator.CuratorTests

/**
  * Created by Ivan Kudryavtsev on 04.08.17.
  */
class OffsetKeeperTests extends CuratorTests {
  val TOPIC = "sample"

  it should "store and load offsets properly" in {
    val offsets = Map(0 -> 1L, 1 -> 100L, 2 -> 10L)
    val offsetKeeper = new ZookeeperOffsetKeeper
    offsetKeeper.store(TOPIC, offsets = offsets)
    offsetKeeper.load(TOPIC, offsets.keys.toSet) shouldBe offsets
  }

  it should "load offsets when partitions are missing properly" in {
    val offsets = Set(3, 4)
    val offsetKeeper = new ZookeeperOffsetKeeper
    offsetKeeper.load(TOPIC, offsets) shouldBe Map (3 -> 0L, 4 -> 0L)
  }

}
