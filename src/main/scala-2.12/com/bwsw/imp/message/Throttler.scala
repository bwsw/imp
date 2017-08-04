package com.bwsw.imp.message

/**
  * Created by Ivan Kudryavtsev on 03.08.17.
  */
trait Throttler {
  def passIngress(message: Message): Boolean
  def passEgress(message: Message): Boolean
}
