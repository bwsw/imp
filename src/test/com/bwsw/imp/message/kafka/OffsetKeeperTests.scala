package com.bwsw.imp.message.kafka

import com.bwsw.cloudstack.common.curator.CuratorTests

/**
  * Created by Ivan Kudryavtsev on 04.08.17.
  */
class OffsetKeeperTests extends CuratorTests {
  val TOPIC = "sample"

  it should "store and load offsets properly" in {
    val offsets = Map(0 -> 1L, 1 -> 100L, 2 -> 10L)
    val topic = "sample"
    val offsetKeeper = new OffsetKeeper(topic)
    offsetKeeper.store(offsets = offsets)
    offsetKeeper.load(offsets.keys.toSet) shouldBe offsets
  }

  it should "load offsets when partitions are missing properly" in {
    val offsets = Set(3, 4)
    val offsetKeeper = new OffsetKeeper(TOPIC)
    offsetKeeper.load(offsets) shouldBe Map (3 -> 0L, 4 -> 0L)
  }

}
