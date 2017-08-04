package com.bwsw.imp.message

/**
  * Created by Ivan Kudryavtsev on 04.08.17.
  */
trait DelayedMessage extends Message {
  def delay: Long
}
