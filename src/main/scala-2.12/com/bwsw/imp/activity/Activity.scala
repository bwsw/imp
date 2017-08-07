package com.bwsw.imp.activity

import com.bwsw.imp.message.Message

/**
  * Created by Ivan Kudryavtsev on 01.08.17.
  */
abstract class Activity extends Message {
  var iteration: Int = 0

  override def hash = s"$uuid-$iteration"

  def activate(e: Environment): Seq[Activity]

  private[imp] def activateIt(e: Environment): Seq[Activity] = {
    iteration += 1
    activate(e)
  }
}
