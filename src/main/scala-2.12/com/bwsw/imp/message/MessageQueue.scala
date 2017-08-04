package com.bwsw.imp.message

/**
  * Created by Ivan Kudryavtsev on 03.08.17.
  */
trait MessageQueue {
  def get: Option[Message]
  def put(message: Message, delay: Long): Unit
  def put(message: DelayedMessage)
}
