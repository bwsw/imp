package com.bwsw.imp.message

/**
  * Created by Ivan Kudryavtsev on 07.08.17.
  */
class FilteredMessageQueue(backendQueue: MessageQueue, filter: MessageFilter) extends MessageQueue {

  override def get: Seq[Message] = {
    val messages = backendQueue.get
    val result = messages.flatMap(message => if(filter.filterGet(message)) Seq(message) else Seq.empty)
    result
  }

  override def put(message: Message): Unit = {
    if(filter.filterPut(message))
      backendQueue.put(message)
  }

  override def putDelayed(message: DelayedMessage): Unit = {
    if(filter.filterPut(message))
      backendQueue.putDelayed(message)
  }

  override def saveOffsets(): Unit = backendQueue.saveOffsets()

  override def loadOffsets(): Unit = backendQueue.loadOffsets()
}
