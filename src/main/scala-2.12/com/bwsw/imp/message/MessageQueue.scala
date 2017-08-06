package com.bwsw.imp.message

/**
  * Created by Ivan Kudryavtsev on 03.08.17.
  */
trait MessageQueue {
  protected def getReadyTime = System.currentTimeMillis()
  def saveOffsets: Unit
  def loadOffsets: Unit
  def get: Seq[Message]
  def put(message: Message): Unit
  def putDelayed(message: DelayedMessage)
}
