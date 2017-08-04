package com.bwsw.imp.message

/**
  * Created by Ivan Kudryavtsev on 04.08.17.
  */
class BypassThrottler extends Throttler {
  override def passIngress(message: Message): Boolean = true

  override def passEgress(message: Message): Boolean = true
}
