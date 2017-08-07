package com.bwsw.imp.message.memory

import java.util.concurrent.LinkedBlockingQueue

import com.bwsw.imp.message.{DelayedMessage, DelayedMessagesCpuProtection, Message, MessageQueue}


/**
  * Created by Ivan Kudryavtsev on 06.08.17.
  */

class MemoryMessageQueue extends MessageQueue with DelayedMessagesCpuProtection {
  val queue = new LinkedBlockingQueue[(Long, Message)]()

  override def saveOffsets(): Unit = {}

  override def get: Seq[Message] = {
    delay()
    val elt = queue.poll()
    if(getReadyTime < elt._1) {
      queue.put(elt)
      incrementCpuProtectionDelay()
      Nil
    } else {
      resetCpuProtectionDelay()
      List(elt._2)
    }
  }

  override def put(message: Message): Unit = queue.put((0, message))

  override def putDelayed(message: DelayedMessage): Unit = queue.put((message.delay, message))

  override def loadOffsets(): Unit = {}
}
