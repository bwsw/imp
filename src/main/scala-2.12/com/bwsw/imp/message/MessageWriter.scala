package com.bwsw.imp.message

/**
  * Created by Ivan Kudryavtsev on 07.08.17.
  */
trait MessageWriter {
  def put(message: Message): Unit
  def putDelayed(message: DelayedMessage)
}
