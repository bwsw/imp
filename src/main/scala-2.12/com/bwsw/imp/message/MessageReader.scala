package com.bwsw.imp.message

/**
  * Created by Ivan Kudryavtsev on 07.08.17.
  */
trait MessageReader {
  protected def getReadyTime = System.currentTimeMillis()
  def get: Seq[Message]
  def saveOffsets: Unit
  def loadOffsets: Unit
}
