package com.bwsw.imp.message

import com.bwsw.imp.message.memory.MemoryMessageQueue
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 10.08.17.
  */
class FilteredMessageQueueTests extends FlatSpec with Matchers {
  it should "filter messages" in {
    val mq = new FilteredMessageQueue(new MemoryMessageQueue, new BypassMessageFilter)
    mq.get shouldBe Nil
    val m = new Message {}
    mq.put(m)
    mq.get shouldBe Seq(m)

    val m2 = new DelayedMessage {
      override def delay: Long = 1
    }

    mq.putDelayed(m2)
    mq.get shouldBe Seq(m2)

    mq.saveOffsets()
    mq.loadOffsets()
  }
}
