package com.bwsw.imp.message.memory

import com.bwsw.imp.message.{DelayedMessage, DelayedMessagesCpuProtection, Message, MessageQueue}

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 06.08.17.
  */
class MemoryMessageQueue extends MessageQueue with DelayedMessagesCpuProtection {
  val queue = mutable.Queue[(Long, Message)]()

  override def saveOffsets: Unit = {}

  override def get: Seq[Message] = {
    delay()
    if(queue.isEmpty) {
      incrementCpuProtectionDelay()
      Nil
    }
    else {
      val elt = queue.dequeue()
      if(getReadyTime < elt._1) {
        queue.enqueue(elt)
        incrementCpuProtectionDelay()
        Nil
      } else {
        resetCpuProtectionDelay()
        List(elt._2)
      }
    }
  }

  override def put(message: Message): Unit = queue.enqueue((0, message))

  override def putDelayed(message: DelayedMessage): Unit = queue.enqueue((message.delay, message))

  override def loadOffsets: Unit = {}
}
