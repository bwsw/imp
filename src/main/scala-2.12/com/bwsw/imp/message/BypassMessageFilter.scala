package com.bwsw.imp.message

/**
  * Created by Ivan Kudryavtsev on 04.08.17.
  */
class BypassMessageFilter extends MessageFilter {
  override def filterGet(message: Message): Boolean = true

  override def filterPut(message: Message): Boolean = true
}
