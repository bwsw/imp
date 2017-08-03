package com.bwsw.imp.message

/**
  * Created by ivan on 03.08.17.
  */
trait Throttler {
  def incoming(message: Message): Boolean
  def outgoing(message: Message): Boolean
}
