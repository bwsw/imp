package com.bwsw.imp.message.memory

import com.bwsw.imp.message.{DelayedMessage, Message}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 06.08.17.
  */
class MemoryMessageQueueTests extends FlatSpec with Matchers {
  it should "Fetch ready messages from queue" in {
    val ramQueue = new MemoryMessageQueue
    val message = new Message {}
    ramQueue.put(message)
    ramQueue.get shouldBe Seq(message)
  }

  it should "forward future messages to  queue" in {
    val ramQueue = new MemoryMessageQueue
    val message = new DelayedMessage {
      override def delay: Long = System.currentTimeMillis() + 1000
    }
    ramQueue.putDelayed(message)
    ramQueue.get shouldBe Nil
    ramQueue.queue.isEmpty shouldBe false
    ramQueue.cpuProtectionDelay shouldBe ramQueue.cpuProtectionDelayIncrement

  }
}
