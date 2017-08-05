package com.bwsw.imp.message

/**
  * Created by Ivan Kudryavtsev on 03.08.17.
  */
trait MessageFilter {
  def filterGet(message: Message): Boolean
  def filterPut(message: Message): Boolean
}
