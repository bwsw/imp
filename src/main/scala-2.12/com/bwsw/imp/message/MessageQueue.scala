package com.bwsw.imp.message

/**
  * Created by Ivan Kudryavtsev on 03.08.17.
  */
trait MessageQueue extends MessageWriter with MessageReader

object MessageQueue {
  val POLLING_INTERVAL = 1000
}