package com.bwsw.imp.message

import com.bwsw.imp.message.memory.MemoryMessageQueue
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 10.08.17.
  */
class FilteredMessageQueueTests extends FlatSpec with Matchers {
  it should "filter messages" in {
    val mq = new FilteredMessageQueue(new MemoryMessageQueue, new DeduplicatingMessageFilter(new MessageCache {
      val cache = mutable.Set[Message]()
      override def checkExists(m: Message): Boolean = {
        if(cache.contains(m))
          true
        else {
          cache.add(m)
          false
        }
      }
    }))

    mq.get shouldBe Nil
    val m = new Message {}
    mq.put(m)
    mq.get shouldBe Seq(m)

    mq.put(m)
    mq.get shouldBe Nil

    val m2 = new DelayedMessage {
      override def delay: Long = 1
    }

    mq.putDelayed(m2)
    mq.get shouldBe Seq(m2)

    mq.saveOffsets()
    mq.loadOffsets()
  }
}
