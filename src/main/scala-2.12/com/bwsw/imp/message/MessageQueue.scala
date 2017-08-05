package com.bwsw.imp.message

/**
  * Created by Ivan Kudryavtsev on 03.08.17.
  */
trait MessageQueue {
  def get: Seq[Message]
  def put(message: Message): Unit
  def put(message: DelayedMessage)
}
